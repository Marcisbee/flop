package server

import (
	"bytes"
	"fmt"
	"html/template"
	"net"
	"net/smtp"
	"sync"
)

// SMTPConfig holds SMTP server settings for sending emails.
type SMTPConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	From     string
}

// EmailTemplateData is passed to email templates when rendering.
type EmailTemplateData struct {
	AppName string
	URL     string
	Token   string
	Email   string
}

// Mailer sends emails via SMTP with customizable templates.
type Mailer struct {
	config    SMTPConfig
	mu        sync.RWMutex
	templates map[string]*template.Template
}

// NewMailer creates a new Mailer with default templates.
func NewMailer(config SMTPConfig) *Mailer {
	return &Mailer{
		config:    config,
		templates: defaultEmailTemplates(),
	}
}

// Send sends an email with the given subject and HTML body.
func (m *Mailer) Send(to, subject, htmlBody string) error {
	addr := net.JoinHostPort(m.config.Host, fmt.Sprintf("%d", m.config.Port))

	msg := fmt.Sprintf(
		"From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/html; charset=UTF-8\r\n\r\n%s",
		m.config.From, to, subject, htmlBody,
	)

	var auth smtp.Auth
	if m.config.Username != "" {
		auth = smtp.PlainAuth("", m.config.Username, m.config.Password, m.config.Host)
	}

	return smtp.SendMail(addr, auth, m.config.From, []string{to}, []byte(msg))
}

// SetTemplate overrides a named email template.
func (m *Mailer) SetTemplate(name string, tmpl *template.Template) {
	m.mu.Lock()
	m.templates[name] = tmpl
	m.mu.Unlock()
}

// RenderTemplate renders a named template with the given data.
func (m *Mailer) RenderTemplate(name string, data EmailTemplateData) (string, error) {
	m.mu.RLock()
	tmpl, ok := m.templates[name]
	m.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("email template %q not found", name)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// SendTemplate renders and sends a named email template.
func (m *Mailer) SendTemplate(to, subject, templateName string, data EmailTemplateData) error {
	body, err := m.RenderTemplate(templateName, data)
	if err != nil {
		return err
	}
	return m.Send(to, subject, body)
}

func defaultEmailTemplates() map[string]*template.Template {
	templates := make(map[string]*template.Template)

	templates["verification"] = template.Must(template.New("verification").Parse(`<!DOCTYPE html>
<html>
<body style="font-family: sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
<h2>Verify your email</h2>
<p>Click the link below to verify your email address:</p>
<p><a href="{{.URL}}" style="display: inline-block; padding: 12px 24px; background: #2563eb; color: #fff; text-decoration: none; border-radius: 6px;">Verify Email</a></p>
<p style="color: #666; font-size: 14px;">If you did not create an account, you can ignore this email.</p>
</body>
</html>`))

	templates["email-change"] = template.Must(template.New("email-change").Parse(`<!DOCTYPE html>
<html>
<body style="font-family: sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
<h2>Confirm email change</h2>
<p>Click the link below to confirm changing your email to <strong>{{.Email}}</strong>:</p>
<p><a href="{{.URL}}" style="display: inline-block; padding: 12px 24px; background: #2563eb; color: #fff; text-decoration: none; border-radius: 6px;">Confirm Email Change</a></p>
<p style="color: #666; font-size: 14px;">If you did not request this change, you can ignore this email.</p>
</body>
</html>`))

	templates["password-reset"] = template.Must(template.New("password-reset").Parse(`<!DOCTYPE html>
<html>
<body style="font-family: sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
<h2>Reset your password</h2>
<p>Click the link below to reset your password:</p>
<p><a href="{{.URL}}" style="display: inline-block; padding: 12px 24px; background: #2563eb; color: #fff; text-decoration: none; border-radius: 6px;">Reset Password</a></p>
<p style="color: #666; font-size: 14px;">If you did not request a password reset, you can ignore this email. This link expires in 1 hour.</p>
</body>
</html>`))

	return templates
}
