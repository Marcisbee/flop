package flop

import (
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"strings"

	"github.com/marcisbee/flop/internal/server"
)

const (
	emailSettingsRelPath = "_system/email.json"
	emailPasswordMask    = "******"
)

var emailTemplateNames = []string{"verification", "email-change", "password-reset"}

type EmailSMTPSettings struct {
	Host       string `json:"host"`
	Port       int    `json:"port"`
	Username   string `json:"username"`
	Password   string `json:"password,omitempty"`
	TLS        string `json:"tls"`
	AuthMethod string `json:"authMethod"`
	LocalName  string `json:"localName"`
}

type EmailTemplateSettings struct {
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

type EmailSettings struct {
	AppName       string                           `json:"appName"`
	AppURL        string                           `json:"appURL"`
	SenderName    string                           `json:"senderName"`
	SenderAddress string                           `json:"senderAddress"`
	UseSMTP       bool                             `json:"useSMTP"`
	SMTP          EmailSMTPSettings                `json:"smtp"`
	Templates     map[string]EmailTemplateSettings `json:"templates"`
}

func defaultEmailSettings(cfg *SMTPConfig) EmailSettings {
	settings := EmailSettings{
		AppName: "",
		AppURL:  "",
		SMTP: EmailSMTPSettings{
			Port:       587,
			TLS:        "auto",
			AuthMethod: "plain",
		},
		Templates: defaultEmailTemplateSettings(),
	}
	if cfg == nil {
		return settings
	}
	normalized := serverConfigFromSMTP(cfg)
	settings.SenderName = normalized.SenderName
	settings.SenderAddress = normalized.SenderAddress
	settings.UseSMTP = normalized.Host != ""
	settings.SMTP = EmailSMTPSettings{
		Host:       normalized.Host,
		Port:       normalized.Port,
		Username:   normalized.Username,
		Password:   normalized.Password,
		TLS:        normalized.TLS,
		AuthMethod: normalized.AuthMethod,
		LocalName:  normalized.LocalName,
	}
	return normalizeEmailSettings(settings)
}

func defaultEmailTemplateSettings() map[string]EmailTemplateSettings {
	mailer := server.NewMailer(server.SMTPConfig{})
	templates := mailer.TemplateDefinitions()
	out := make(map[string]EmailTemplateSettings, len(templates))
	for _, name := range emailTemplateNames {
		def := templates[name]
		out[name] = EmailTemplateSettings{
			Subject: def.Subject,
			Body:    def.Body,
		}
	}
	return out
}

func normalizeEmailSettings(settings EmailSettings) EmailSettings {
	settings.AppName = strings.TrimSpace(settings.AppName)
	settings.AppURL = strings.TrimSpace(settings.AppURL)
	settings.SenderName = strings.TrimSpace(settings.SenderName)
	settings.SenderAddress = strings.TrimSpace(settings.SenderAddress)
	settings.SMTP.Host = strings.TrimSpace(settings.SMTP.Host)
	settings.SMTP.Username = strings.TrimSpace(settings.SMTP.Username)
	settings.SMTP.Password = strings.TrimSpace(settings.SMTP.Password)
	settings.SMTP.LocalName = strings.TrimSpace(settings.SMTP.LocalName)
	settings.SMTP.TLS = strings.ToLower(strings.TrimSpace(settings.SMTP.TLS))
	settings.SMTP.AuthMethod = strings.ToLower(strings.TrimSpace(settings.SMTP.AuthMethod))
	if settings.SMTP.Port <= 0 {
		settings.SMTP.Port = 587
	}
	if settings.SMTP.TLS == "" {
		settings.SMTP.TLS = "auto"
	}
	if settings.SMTP.AuthMethod == "" {
		settings.SMTP.AuthMethod = "plain"
	}
	if settings.Templates == nil {
		settings.Templates = map[string]EmailTemplateSettings{}
	}
	defaults := defaultEmailTemplateSettings()
	for _, name := range emailTemplateNames {
		current := settings.Templates[name]
		def := defaults[name]
		current.Subject = strings.TrimSpace(current.Subject)
		if current.Subject == "" {
			current.Subject = def.Subject
		}
		if strings.TrimSpace(current.Body) == "" {
			current.Body = def.Body
		}
		settings.Templates[name] = current
	}
	return settings
}

func validateEmailSettings(settings EmailSettings) error {
	settings = normalizeEmailSettings(settings)
	for _, name := range emailTemplateNames {
		tpl := settings.Templates[name]
		if strings.TrimSpace(tpl.Subject) == "" {
			return fmt.Errorf("%s email subject is required", name)
		}
		if strings.TrimSpace(tpl.Body) == "" {
			return fmt.Errorf("%s email body is required", name)
		}
	}
	if !settings.UseSMTP {
		return nil
	}
	switch {
	case settings.SenderAddress == "":
		return fmt.Errorf("sender address is required")
	case settings.SMTP.Host == "":
		return fmt.Errorf("smtp host is required")
	case settings.SMTP.Port <= 0:
		return fmt.Errorf("smtp port is required")
	}
	switch settings.SMTP.TLS {
	case "auto", "starttls", "ssl", "none":
	default:
		return fmt.Errorf("unsupported tls mode %q", settings.SMTP.TLS)
	}
	switch settings.SMTP.AuthMethod {
	case "plain", "login", "cram-md5", "none":
	default:
		return fmt.Errorf("unsupported auth method %q", settings.SMTP.AuthMethod)
	}
	if settings.SMTP.AuthMethod != "none" && settings.SMTP.Username != "" && settings.SMTP.Password == "" {
		return fmt.Errorf("smtp password is required")
	}
	return nil
}

func cloneEmailSettings(settings EmailSettings, maskPassword bool) EmailSettings {
	settings = normalizeEmailSettings(settings)
	out := EmailSettings{
		AppName:       settings.AppName,
		AppURL:        settings.AppURL,
		SenderName:    settings.SenderName,
		SenderAddress: settings.SenderAddress,
		UseSMTP:       settings.UseSMTP,
		SMTP:          settings.SMTP,
		Templates:     make(map[string]EmailTemplateSettings, len(settings.Templates)),
	}
	if maskPassword && out.SMTP.Password != "" {
		out.SMTP.Password = emailPasswordMask
	}
	for key, value := range settings.Templates {
		out.Templates[key] = value
	}
	return out
}

func loadEmailSettings(dataDir string, fallback EmailSettings) (EmailSettings, error) {
	path := filepath.Join(dataDir, emailSettingsRelPath)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return normalizeEmailSettings(fallback), nil
		}
		return EmailSettings{}, err
	}
	settings := fallback
	if err := json.Unmarshal(data, &settings); err != nil {
		return EmailSettings{}, err
	}
	return normalizeEmailSettings(settings), nil
}

func saveEmailSettings(dataDir string, settings EmailSettings) error {
	path := filepath.Join(dataDir, emailSettingsRelPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

func serverConfigFromSMTP(cfg *SMTPConfig) server.SMTPConfig {
	if cfg == nil {
		return server.SMTPConfig{}
	}
	return server.SMTPConfig{
		Host:          cfg.Host,
		Port:          cfg.Port,
		Username:      cfg.Username,
		Password:      cfg.Password,
		From:          cfg.From,
		SenderName:    cfg.SenderName,
		SenderAddress: cfg.SenderAddress,
		TLS:           cfg.TLS,
		AuthMethod:    cfg.AuthMethod,
		LocalName:     cfg.LocalName,
	}
}

func buildMailerFromEmailSettings(settings EmailSettings) (*server.Mailer, error) {
	settings = normalizeEmailSettings(settings)
	if err := validateEmailSettings(settings); err != nil {
		return nil, err
	}
	if !settings.UseSMTP {
		return nil, nil
	}
	mailer := server.NewMailer(server.SMTPConfig{
		Host:          settings.SMTP.Host,
		Port:          settings.SMTP.Port,
		Username:      settings.SMTP.Username,
		Password:      settings.SMTP.Password,
		SenderName:    settings.SenderName,
		SenderAddress: settings.SenderAddress,
		TLS:           settings.SMTP.TLS,
		AuthMethod:    settings.SMTP.AuthMethod,
		LocalName:     settings.SMTP.LocalName,
	})
	for _, name := range emailTemplateNames {
		tpl := settings.Templates[name]
		if err := mailer.SetTemplateDefinition(name, server.MessageTemplate{
			Subject: tpl.Subject,
			Body:    tpl.Body,
		}); err != nil {
			return nil, fmt.Errorf("%s template: %w", name, err)
		}
	}
	return mailer, nil
}

func (d *Database) initEmailSettings() error {
	if d == nil || d.app == nil {
		return fmt.Errorf("database app context unavailable")
	}
	settings, err := loadEmailSettings(d.GetDataDir(), defaultEmailSettings(d.app.config.SMTP))
	if err != nil {
		return err
	}
	mailer, err := buildMailerFromEmailSettings(settings)
	if err != nil {
		return err
	}
	d.emailMu.Lock()
	d.emailSettings = settings
	d.mailer = mailer
	d.emailMu.Unlock()
	return nil
}

func (d *Database) getEmailSettings() EmailSettings {
	d.emailMu.RLock()
	defer d.emailMu.RUnlock()
	return cloneEmailSettings(d.emailSettings, true)
}

func (d *Database) rawEmailSettings() EmailSettings {
	d.emailMu.RLock()
	defer d.emailMu.RUnlock()
	return cloneEmailSettings(d.emailSettings, false)
}

func (d *Database) updateEmailSettings(next EmailSettings) (EmailSettings, error) {
	next = normalizeEmailSettings(next)
	if err := validateEmailSettings(next); err != nil {
		return EmailSettings{}, err
	}
	mailer, err := buildMailerFromEmailSettings(next)
	if err != nil {
		return EmailSettings{}, err
	}
	if err := saveEmailSettings(d.GetDataDir(), next); err != nil {
		return EmailSettings{}, err
	}
	d.emailMu.Lock()
	d.emailSettings = next
	d.mailer = mailer
	d.emailMu.Unlock()
	return cloneEmailSettings(next, true), nil
}

func (d *Database) SetEmailTemplate(name string, tmpl *template.Template) {
	if d == nil || tmpl == nil || d.mailer == nil {
		return
	}
	d.emailMu.Lock()
	d.mailer.SetTemplate(name, tmpl)
	d.emailMu.Unlock()
}

func (d *Database) testEmailSettings(settings EmailSettings, to string) error {
	settings = normalizeEmailSettings(settings)
	if err := validateEmailSettings(settings); err != nil {
		return err
	}
	if strings.TrimSpace(to) == "" {
		return fmt.Errorf("test email address is required")
	}
	mailer, err := buildMailerFromEmailSettings(settings)
	if err != nil {
		return err
	}
	if mailer == nil {
		return fmt.Errorf("smtp mail server is disabled")
	}
	subject := "Flop test email"
	body := "<!DOCTYPE html><html><body style=\"font-family: sans-serif; max-width: 640px; margin: 0 auto; padding: 24px;\"><h2>Flop email settings look good</h2><p>This is a test email sent from the Flop admin panel.</p></body></html>"
	return mailer.Send(strings.TrimSpace(to), subject, body)
}

func (d *Database) testEmailTemplate(settings EmailSettings, to, templateName string) error {
	settings = normalizeEmailSettings(settings)
	if err := validateEmailSettings(settings); err != nil {
		return err
	}
	to = strings.TrimSpace(to)
	templateName = strings.TrimSpace(templateName)
	if to == "" {
		return fmt.Errorf("test email address is required")
	}
	if templateName == "" {
		return fmt.Errorf("email template is required")
	}
	mailer, err := buildMailerFromEmailSettings(settings)
	if err != nil {
		return err
	}
	if mailer == nil {
		return fmt.Errorf("smtp mail server is disabled")
	}
	return mailer.SendTemplate(to, "", templateName, buildEmailTemplateData(settings, templateName, to, "test-token-123"))
}

func buildEmailActionURL(baseURL, templateName, token string) string {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = "https://example.com"
	}
	switch templateName {
	case "verification":
		return baseURL + "/verify?token=" + token
	case "email-change":
		return baseURL + "/confirm-email-change?token=" + token
	case "password-reset":
		return baseURL + "/reset-password?token=" + token
	default:
		return baseURL + "/_/mail/test?token=" + token
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func buildEmailTemplateData(settings EmailSettings, templateName, email, token string) server.EmailTemplateData {
	appURL := strings.TrimRight(strings.TrimSpace(settings.AppURL), "/")
	return server.EmailTemplateData{
		AppName: firstNonEmpty(settings.AppName, settings.SenderName),
		AppURL:  appURL,
		URL:     buildEmailActionURL(appURL, templateName, token),
		Token:   token,
		Email:   strings.TrimSpace(email),
	}
}

func (d *Database) sendAuthTemplateEmail(templateName, to, email, token string) error {
	if d == nil {
		return fmt.Errorf("database not available")
	}
	d.emailMu.RLock()
	mailer := d.mailer
	settings := cloneEmailSettings(d.emailSettings, false)
	d.emailMu.RUnlock()
	if mailer == nil {
		return nil
	}
	return mailer.SendTemplate(strings.TrimSpace(to), "", templateName, buildEmailTemplateData(settings, templateName, email, token))
}
