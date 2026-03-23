package flop

import "testing"

func TestNormalizeEmailSettingsAddsDefaultTemplates(t *testing.T) {
	settings := normalizeEmailSettings(EmailSettings{
		UseSMTP: false,
	})

	for _, name := range emailTemplateNames {
		tpl, ok := settings.Templates[name]
		if !ok {
			t.Fatalf("expected template %q to be present", name)
		}
		if tpl.Subject == "" {
			t.Fatalf("expected template %q to have default subject", name)
		}
		if tpl.Body == "" {
			t.Fatalf("expected template %q to have default body", name)
		}
	}
}

func TestCloneEmailSettingsMasksPassword(t *testing.T) {
	settings := EmailSettings{
		SenderName:    "Test Sender",
		SenderAddress: "system@example.com",
		UseSMTP:       true,
		SMTP: EmailSMTPSettings{
			Host:       "smtp.example.com",
			Port:       587,
			Username:   "system@example.com",
			Password:   "super-secret",
			TLS:        "auto",
			AuthMethod: "plain",
		},
	}

	masked := cloneEmailSettings(settings, true)
	if masked.SMTP.Password != emailPasswordMask {
		t.Fatalf("expected masked password, got %q", masked.SMTP.Password)
	}

	raw := cloneEmailSettings(settings, false)
	if raw.SMTP.Password != "super-secret" {
		t.Fatalf("expected raw password to be preserved, got %q", raw.SMTP.Password)
	}
}

