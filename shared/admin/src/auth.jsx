import React, { useState, useEffect } from "react";
import { createRoot } from "react-dom/client";
import { Button } from "@cloudflare/kumo/components/button";
import { Input } from "@cloudflare/kumo/components/input";
import { Select } from "@cloudflare/kumo/components/select";
import { LayerCard } from "@cloudflare/kumo/components/layer-card";
import { Banner } from "@cloudflare/kumo/components/banner";
import { DatabaseIcon } from "@phosphor-icons/react";
import { ThemeControl } from "./ui.jsx";
import "@cloudflare/kumo/styles/standalone";
import "./layout.css";
function Auth() {
  const [email, setEmail] = useState(""),
    [password, setPassword] = useState(""),
    [confirmation, setConfirmation] = useState("");
  const [fields, setFields] = useState([]),
    [extra, setExtra] = useState({}),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false),
    [schemaError, setSchemaError] = useState("");
  const token = new URLSearchParams(location.search).get("token") || "";
  async function loadSchema() {
    setSchemaError("");
    try {
      const response = await fetch(
        "/_/api/setup-schema?token=" + encodeURIComponent(token),
      );
      const data = await response.json();
      if (!response.ok)
        throw Error(data.error || "Unable to load setup fields");
      setFields(data.fields || []);
    } catch (e) {
      setSchemaError(e.message);
    }
  }
  useEffect(() => {
    if (__SETUP__) loadSchema();
  }, []);
  async function submit(event) {
    event.preventDefault();
    setError("");
    if (__SETUP__ && password !== confirmation) {
      setError("Passwords do not match");
      return;
    }
    setBusy(true);
    try {
      const body = { email, password };
      if (__SETUP__) {
        body.token = token;
        for (const field of fields) {
          const value = extra[field.name];
          if (value !== undefined && value !== "")
            body[field.name] = ["number", "integer"].includes(field.type)
              ? Number(value)
              : value;
        }
      }
      const response = await fetch(
        "/_/api/" + (__SETUP__ ? "setup" : "login"),
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        },
      );
      const data = await response.json();
      if (!response.ok) throw Error(data.error || "Unable to sign in");
      if (__SETUP__) {
        location.href = "/_/login";
        return;
      }
      localStorage.setItem("flop_admin_token", data.token);
      if (data.refreshToken)
        localStorage.setItem("flop_admin_refresh", data.refreshToken);
      location.href = "/_";
    } catch (e) {
      setError(e.message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="auth-page">
      <header className="auth-bar">
        <div className="brand">
          <DatabaseIcon size={24} />
          Flop
        </div>
        <ThemeControl />
      </header>
      <div className="auth-content">
        <div className="auth-heading">
          <h1>{__SETUP__ ? "Set up your workspace" : "Sign in to Flop"}</h1>
          <p className="muted">
            {__SETUP__
              ? "Create an admin account to manage your database."
              : "Your database, records, and services in one place."}
          </p>
        </div>
        <LayerCard className="auth-card">
          <form onSubmit={submit} className="auth-form">
            <Input
              label="Email"
              id="email"
              type="email"
              autoComplete="username"
              required
              autoFocus
              value={email}
              onChange={(e) => setEmail(e.target.value)}
            />
            {schemaError && (
              <Banner variant="error">
                {schemaError}
                <Button onClick={loadSchema}>Try again</Button>
              </Banner>
            )}
            {fields.map((f) =>
              f.type === "enum" && f.enumValues?.length ? (
                <Select
                  key={f.name}
                  label={f.name}
                  required={f.required}
                  value={extra[f.name] || ""}
                  onValueChange={(v) => setExtra({ ...extra, [f.name]: v })}
                >
                  <Select.Option value="">Select…</Select.Option>
                  {f.enumValues.map((v) => (
                    <Select.Option key={v} value={v}>
                      {v}
                    </Select.Option>
                  ))}
                </Select>
              ) : (
                <Input
                  key={f.name}
                  label={f.name}
                  required={f.required}
                  type={
                    ["number", "integer"].includes(f.type) ? "number" : "text"
                  }
                  value={extra[f.name] || ""}
                  onChange={(e) =>
                    setExtra({ ...extra, [f.name]: e.target.value })
                  }
                />
              ),
            )}
            <Input
              label="Password"
              id="password"
              type="password"
              autoComplete={__SETUP__ ? "new-password" : "current-password"}
              required
              minLength={__SETUP__ ? 6 : undefined}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
            />
            {__SETUP__ && (
              <Input
                label="Confirm password"
                type="password"
                autoComplete="new-password"
                required
                minLength={6}
                value={confirmation}
                onChange={(e) => setConfirmation(e.target.value)}
              />
            )}
            {error && (
              <div id="error" role="alert">
                <Banner variant="error">{error}</Banner>
              </div>
            )}
            <Button
              type="submit"
              variant="primary"
              disabled={busy || !!schemaError}
            >
              {busy ? "Please wait…" : __SETUP__ ? "Create admin" : "Sign in"}
            </Button>
          </form>
        </LayerCard>
        <p className="muted auth-caption">Flop database administration</p>
      </div>
    </div>
  );
}
createRoot(document.getElementById("app")).render(<Auth />);
