import { renderAdminPage, renderLoginPage, renderSetupPage } from "../../../src/admin/ui.ts";

const out = `package server

// Code generated from src/admin/ui.ts; DO NOT EDIT.

const adminLoginHTML = ${JSON.stringify(renderLoginPage())}

const adminSetupHTML = ${JSON.stringify(renderSetupPage())}

const adminPageHTML = ${JSON.stringify(renderAdminPage())}
`;

await Deno.writeTextFile(new URL("./admin_ui_generated.go", import.meta.url), out);
console.log("generated go/internal/server/admin_ui_generated.go");
