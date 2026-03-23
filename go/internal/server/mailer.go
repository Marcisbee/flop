package server

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"html"
	"html/template"
	"mime"
	"net"
	"net/mail"
	"net/smtp"
	"strings"
	"sync"
	"time"
)

// SMTPConfig holds SMTP server settings for sending emails.
type SMTPConfig struct {
	Host          string
	Port          int
	Username      string
	Password      string
	From          string
	SenderName    string
	SenderAddress string
	TLS           string
	AuthMethod    string
	LocalName     string
}

// EmailTemplateData is passed to email templates when rendering.
type EmailTemplateData struct {
	AppName string
	URL     string
	Token   string
	Email   string
}

type MessageTemplate struct {
	Subject string
	Body    string
}

type compiledMessageTemplate struct {
	raw         MessageTemplate
	subjectTmpl *template.Template
	bodyTmpl    *template.Template
}

// Mailer sends emails via SMTP with customizable templates.
type Mailer struct {
	config    SMTPConfig
	mu        sync.RWMutex
	templates map[string]compiledMessageTemplate
}

// NewMailer creates a new Mailer with default templates.
func NewMailer(config SMTPConfig) *Mailer {
	normalized := normalizeSMTPConfig(config)
	return &Mailer{
		config:    normalized,
		templates: defaultEmailTemplates(),
	}
}

// Send sends an email with the given subject and HTML body.
func (m *Mailer) Send(to, subject, htmlBody string) error {
	cfg := normalizeSMTPConfig(m.config)
	if err := validateSMTPConfig(cfg); err != nil {
		return err
	}
	fromHeader := formatFromHeader(cfg)
	msg := fmt.Sprintf(
		"From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/html; charset=UTF-8\r\n\r\n%s",
		fromHeader,
		to,
		encodeRFC2047(subject),
		htmlBody,
	)

	client, conn, err := newSMTPClient(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	defer client.Quit()

	if err := smtpAuthenticate(client, cfg); err != nil {
		return err
	}
	if err := client.Mail(cfg.SenderAddress); err != nil {
		return err
	}
	if err := client.Rcpt(to); err != nil {
		return err
	}
	writer, err := client.Data()
	if err != nil {
		return err
	}
	if _, err := writer.Write([]byte(msg)); err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}

func (m *Mailer) TestConnection() error {
	cfg := normalizeSMTPConfig(m.config)
	if err := validateSMTPConfig(cfg); err != nil {
		return err
	}
	client, conn, err := newSMTPClient(cfg)
	if err != nil {
		return err
	}
	defer conn.Close()
	defer client.Quit()
	return smtpAuthenticate(client, cfg)
}

// SetTemplate overrides a named email template.
func (m *Mailer) SetTemplate(name string, tmpl *template.Template) {
	m.mu.Lock()
	current, ok := m.templates[name]
	if !ok {
		current = defaultTemplateDefinition(name)
	}
	m.templates[name] = compiledMessageTemplate{
		raw: MessageTemplate{
			Subject: current.raw.Subject,
			Body:    current.raw.Body,
		},
		subjectTmpl: current.subjectTmpl,
		bodyTmpl:    tmpl,
	}
	m.mu.Unlock()
}

func (m *Mailer) SetTemplateDefinition(name string, def MessageTemplate) error {
	compiled, err := compileMessageTemplate(name, def)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.templates[name] = compiled
	m.mu.Unlock()
	return nil
}

func (m *Mailer) TemplateDefinitions() map[string]MessageTemplate {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]MessageTemplate, len(m.templates))
	for name, tmpl := range m.templates {
		out[name] = tmpl.raw
	}
	return out
}

// RenderTemplate renders a named template with the given data.
func (m *Mailer) RenderTemplate(name string, data EmailTemplateData) (string, error) {
	_, body, err := m.RenderTemplateMessage(name, data)
	return body, err
}

func (m *Mailer) RenderTemplateMessage(name string, data EmailTemplateData) (string, string, error) {
	m.mu.RLock()
	tmpl, ok := m.templates[name]
	m.mu.RUnlock()
	if !ok {
		return "", "", fmt.Errorf("email template %q not found", name)
	}

	var subject bytes.Buffer
	if err := tmpl.subjectTmpl.Execute(&subject, data); err != nil {
		return "", "", err
	}
	var body bytes.Buffer
	if err := tmpl.bodyTmpl.Execute(&body, data); err != nil {
		return "", "", err
	}
	return strings.TrimSpace(subject.String()), body.String(), nil
}

// SendTemplate renders and sends a named email template.
func (m *Mailer) SendTemplate(to, subject, templateName string, data EmailTemplateData) error {
	renderedSubject, body, err := m.RenderTemplateMessage(templateName, data)
	if err != nil {
		return err
	}
	if strings.TrimSpace(subject) == "" {
		subject = renderedSubject
	}
	return m.Send(to, subject, body)
}

func defaultEmailTemplates() map[string]compiledMessageTemplate {
	templates := make(map[string]compiledMessageTemplate)

	templates["verification"] = mustCompileMessageTemplate("verification", MessageTemplate{
		Subject: "Verify your email",
		Body: `<!DOCTYPE html>
<html>
<body style="font-family: sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
<h2>Verify your email</h2>
<p>Click the link below to verify your email address:</p>
<p><a href="{{.URL}}" style="display: inline-block; padding: 12px 24px; background: #2563eb; color: #fff; text-decoration: none; border-radius: 6px;">Verify Email</a></p>
<p style="color: #666; font-size: 14px;">If you did not create an account, you can ignore this email.</p>
</body>
</html>`,
	})

	templates["email-change"] = mustCompileMessageTemplate("email-change", MessageTemplate{
		Subject: "Confirm email change",
		Body: `<!DOCTYPE html>
<html>
<body style="font-family: sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
<h2>Confirm email change</h2>
<p>Click the link below to confirm changing your email to <strong>{{.Email}}</strong>:</p>
<p><a href="{{.URL}}" style="display: inline-block; padding: 12px 24px; background: #2563eb; color: #fff; text-decoration: none; border-radius: 6px;">Confirm Email Change</a></p>
<p style="color: #666; font-size: 14px;">If you did not request this change, you can ignore this email.</p>
</body>
</html>`,
	})

	templates["password-reset"] = mustCompileMessageTemplate("password-reset", MessageTemplate{
		Subject: "Reset your password",
		Body: `<!DOCTYPE html>
<html>
<body style="font-family: sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
<h2>Reset your password</h2>
<p>Click the link below to reset your password:</p>
<p><a href="{{.URL}}" style="display: inline-block; padding: 12px 24px; background: #2563eb; color: #fff; text-decoration: none; border-radius: 6px;">Reset Password</a></p>
<p style="color: #666; font-size: 14px;">If you did not request a password reset, you can ignore this email. This link expires in 1 hour.</p>
</body>
</html>`,
	})

	return templates
}

func defaultTemplateDefinition(name string) compiledMessageTemplate {
	if tmpl, ok := defaultEmailTemplates()[name]; ok {
		return tmpl
	}
	return mustCompileMessageTemplate(name, MessageTemplate{
		Subject: name,
		Body:    "<!DOCTYPE html><html><body></body></html>",
	})
}

func compileMessageTemplate(name string, def MessageTemplate) (compiledMessageTemplate, error) {
	subjectTmpl, err := template.New(name + "_subject").Parse(def.Subject)
	if err != nil {
		return compiledMessageTemplate{}, err
	}
	bodyTmpl, err := template.New(name).Parse(def.Body)
	if err != nil {
		return compiledMessageTemplate{}, err
	}
	return compiledMessageTemplate{
		raw:         def,
		subjectTmpl: subjectTmpl,
		bodyTmpl:    bodyTmpl,
	}, nil
}

func mustCompileMessageTemplate(name string, def MessageTemplate) compiledMessageTemplate {
	compiled, err := compileMessageTemplate(name, def)
	if err != nil {
		panic(err)
	}
	return compiled
}

func normalizeSMTPConfig(cfg SMTPConfig) SMTPConfig {
	cfg.Host = strings.TrimSpace(cfg.Host)
	cfg.Username = strings.TrimSpace(cfg.Username)
	cfg.Password = strings.TrimSpace(cfg.Password)
	cfg.SenderName = strings.TrimSpace(cfg.SenderName)
	cfg.SenderAddress = strings.TrimSpace(cfg.SenderAddress)
	cfg.TLS = strings.ToLower(strings.TrimSpace(cfg.TLS))
	cfg.AuthMethod = strings.ToLower(strings.TrimSpace(cfg.AuthMethod))
	cfg.LocalName = strings.TrimSpace(cfg.LocalName)
	if cfg.Port <= 0 {
		cfg.Port = 587
	}
	if cfg.TLS == "" {
		cfg.TLS = "auto"
	}
	if cfg.AuthMethod == "" {
		cfg.AuthMethod = "plain"
	}
	if cfg.SenderAddress == "" && strings.TrimSpace(cfg.From) != "" {
		if parsed, err := mail.ParseAddress(cfg.From); err == nil {
			cfg.SenderAddress = strings.TrimSpace(parsed.Address)
			if cfg.SenderName == "" {
				cfg.SenderName = strings.TrimSpace(parsed.Name)
			}
		} else {
			cfg.SenderAddress = strings.TrimSpace(cfg.From)
		}
	}
	cfg.From = formatFromHeader(cfg)
	return cfg
}

func validateSMTPConfig(cfg SMTPConfig) error {
	switch {
	case cfg.SenderAddress == "":
		return fmt.Errorf("sender address is required")
	case cfg.Host == "":
		return fmt.Errorf("smtp host is required")
	case cfg.Port <= 0:
		return fmt.Errorf("smtp port is required")
	}
	switch cfg.TLS {
	case "auto", "starttls", "ssl", "none":
	default:
		return fmt.Errorf("unsupported tls mode %q", cfg.TLS)
	}
	switch cfg.AuthMethod {
	case "plain", "login", "cram-md5", "none":
	default:
		return fmt.Errorf("unsupported auth method %q", cfg.AuthMethod)
	}
	return nil
}

func formatFromHeader(cfg SMTPConfig) string {
	addr := mail.Address{
		Name:    cfg.SenderName,
		Address: cfg.SenderAddress,
	}
	if strings.TrimSpace(addr.Address) == "" {
		return strings.TrimSpace(cfg.From)
	}
	return addr.String()
}

func newSMTPClient(cfg SMTPConfig) (*smtp.Client, net.Conn, error) {
	addr := net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port))
	if cfg.TLS == "ssl" {
		conn, err := tls.Dial("tcp", addr, &tls.Config{ServerName: cfg.Host})
		if err != nil {
			return nil, nil, err
		}
		client, err := smtp.NewClient(conn, cfg.Host)
		if err != nil {
			_ = conn.Close()
			return nil, nil, err
		}
		if cfg.LocalName != "" {
			if err := client.Hello(cfg.LocalName); err != nil {
				_ = client.Close()
				_ = conn.Close()
				return nil, nil, err
			}
		}
		return client, conn, nil
	}

	conn, err := net.DialTimeout("tcp", addr, 15*time.Second)
	if err != nil {
		return nil, nil, err
	}
	client, err := smtp.NewClient(conn, cfg.Host)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if cfg.LocalName != "" {
		if err := client.Hello(cfg.LocalName); err != nil {
			_ = client.Close()
			_ = conn.Close()
			return nil, nil, err
		}
	}
	if cfg.TLS == "starttls" || cfg.TLS == "auto" {
		if ok, _ := client.Extension("STARTTLS"); ok {
			if err := client.StartTLS(&tls.Config{ServerName: cfg.Host}); err != nil {
				_ = client.Close()
				_ = conn.Close()
				return nil, nil, err
			}
		} else if cfg.TLS == "starttls" {
			_ = client.Close()
			_ = conn.Close()
			return nil, nil, fmt.Errorf("smtp server does not support STARTTLS")
		}
	}
	return client, conn, nil
}

func smtpAuthenticate(client *smtp.Client, cfg SMTPConfig) error {
	if cfg.Username == "" || cfg.AuthMethod == "none" {
		return nil
	}
	var auth smtp.Auth
	switch cfg.AuthMethod {
	case "plain":
		auth = smtp.PlainAuth("", cfg.Username, cfg.Password, cfg.Host)
	case "login":
		auth = &loginAuth{username: cfg.Username, password: cfg.Password}
	case "cram-md5":
		auth = smtp.CRAMMD5Auth(cfg.Username, cfg.Password)
	default:
		return fmt.Errorf("unsupported auth method %q", cfg.AuthMethod)
	}
	return client.Auth(auth)
}

func encodeRFC2047(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return mime.QEncoding.Encode("utf-8", value)
}

type loginAuth struct {
	username string
	password string
}

func (a *loginAuth) Start(server *smtp.ServerInfo) (string, []byte, error) {
	return "LOGIN", []byte(a.username), nil
}

func (a *loginAuth) Next(fromServer []byte, more bool) ([]byte, error) {
	if !more {
		return nil, nil
	}
	prompt := strings.ToLower(strings.TrimSpace(string(fromServer)))
	switch {
	case strings.Contains(prompt, "username"):
		return []byte(a.username), nil
	case strings.Contains(prompt, "password"):
		return []byte(a.password), nil
	default:
		return nil, fmt.Errorf("unexpected login auth prompt: %s", html.EscapeString(string(fromServer)))
	}
}
