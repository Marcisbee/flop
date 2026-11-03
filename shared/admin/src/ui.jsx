import React, { useState, useEffect, Children, forwardRef } from "react";
import htm from "htm";
import { Button } from "@cloudflare/kumo/components/button";
import { Input, InputArea } from "@cloudflare/kumo/components/input";
import { Select } from "@cloudflare/kumo/components/select";
import { Checkbox } from "@cloudflare/kumo/components/checkbox";
import { Table } from "@cloudflare/kumo/components/table";
import { Badge } from "@cloudflare/kumo/components/badge";
import { LayerCard } from "@cloudflare/kumo/components/layer-card";
import { Dialog } from "@cloudflare/kumo/components/dialog";
import { Switch } from "@cloudflare/kumo/components/switch";
import { Sidebar, useSidebar } from "@cloudflare/kumo/components/sidebar";
import { Tabs } from "@cloudflare/kumo/components/tabs";
import {
  Toasty,
  createKumoToastManager,
} from "@cloudflare/kumo/components/toast";
import { Banner } from "@cloudflare/kumo/components/banner";
import {
  DatabaseIcon,
  TableIcon,
  ImageIcon,
  ChartLineIcon,
  PulseIcon,
  EnvelopeIcon,
  ClockCounterClockwiseIcon,
  FlowArrowIcon,
  SignOutIcon,
  MagnifyingGlassIcon,
  SunIcon,
  MoonIcon,
  ArrowRightIcon,
  ArchiveIcon,
} from "@phosphor-icons/react";
export {
  Banner,
  Table,
  Badge,
  LayerCard,
  Dialog,
  Switch,
  Sidebar,
  Tabs,
  Toasty,
};
export const toastManager = createKumoToastManager();

// The retained data views use HTML templates. Normalize HTML attributes for React.
function templateElement(type, attributes, ...children) {
  const props = { ...attributes };
  const names = {
    class: "className",
    for: "htmlFor",
    tabindex: "tabIndex",
    colspan: "colSpan",
    rowspan: "rowSpan",
    autofocus: "autoFocus",
    readonly: "readOnly",
    "stroke-width": "strokeWidth",
    "stroke-linecap": "strokeLinecap",
    "stroke-linejoin": "strokeLinejoin",
    "fill-rule": "fillRule",
    "clip-rule": "clipRule",
    "stroke-dasharray": "strokeDasharray",
    "stroke-dashoffset": "strokeDashoffset",
  };
  for (const [oldName, newName] of Object.entries(names))
    if (oldName in props) {
      props[newName] = props[oldName];
      delete props[oldName];
    }
  if (typeof props.style === "string")
    props.style = Object.fromEntries(
      props.style
        .split(";")
        .filter((x) => x.includes(":"))
        .map((x) => {
          const i = x.indexOf(":");
          const key = x.slice(0, i).trim();
          return [
            key.startsWith("--")
              ? key
              : key.replace(/-([a-z])/g, (_, c) => c.toUpperCase()),
            x.slice(i + 1).trim(),
          ];
        }),
    );
  return React.createElement(type, props, ...children);
}
export const html = htm.bind(templateElement);

export const Action = forwardRef(function Action(
  { className = "", variant, children, title, ...props },
  ref,
) {
  const kind =
    variant ||
    (className.includes("btn-primary")
      ? "primary"
      : className.includes("danger")
        ? "destructive"
        : className.includes("btn-icon") || className.includes("btn-filter")
          ? "ghost"
          : "secondary");
  return (
    <Button
      ref={ref}
      variant={kind}
      size="base"
      className={className}
      title={title}
      aria-label={
        props["aria-label"] ||
        (className.includes("btn-icon") ? title : undefined)
      }
      {...props}
    >
      {children}
    </Button>
  );
});
export const FieldInput = forwardRef(function FieldInput(
  { type = "text", onInput, onChange, className = "", ...props },
  ref,
) {
  if (type === "file")
    return (
      <input
        ref={ref}
        type="file"
        onChange={onChange}
        className={className}
        {...props}
      />
    );
  if (type === "checkbox")
    return (
      <Checkbox
        ref={ref}
        className={className}
        {...props}
        onCheckedChange={(checked) => onChange?.({ target: { checked } })}
      />
    );
  return (
    <Input
      ref={ref}
      type={type}
      className={className}
      {...props}
      onChange={onChange || onInput}
    />
  );
});
export function FieldSelect({
  onChange,
  children,
  className = "",
  value,
  ...props
}) {
  const items = Children.toArray(children)
    .filter(React.isValidElement)
    .map((child) => ({
      value: child.props.value,
      label: child.props.children,
    }));
  return (
    <Select
      className={"field-select " + className}
      placeholder={items.find((item) => item.value === "")?.label || "Select…"}
      items={items}
      value={value}
      {...props}
      onValueChange={(value) => onChange?.({ target: { value } })}
    >
      {children}
    </Select>
  );
}
FieldSelect.Option = Select.Option;
export function FieldArea({ onInput, onChange, ...props }) {
  return <InputArea {...props} onChange={onChange || onInput} />;
}

let pendingConfirm;
export function confirmAction(message) {
  return new Promise((resolve) => pendingConfirm?.({ message, resolve }));
}
export function ConfirmDialog() {
  const [request, setRequest] = useState(null);
  useEffect(() => {
    pendingConfirm = setRequest;
    return () => {
      pendingConfirm = null;
    };
  }, []);
  const answer = (value) => {
    request?.resolve(value);
    setRequest(null);
  };
  return (
    <Dialog.Root
      open={!!request}
      onOpenChange={(open) => {
        if (!open) answer(false);
      }}
      role="alertdialog"
    >
      <Dialog className="dialog-content">
        <Dialog.Title>Confirm action</Dialog.Title>
        <Dialog.Description className="confirm-message">
          {request?.message}
        </Dialog.Description>
        <div className="dialog-actions">
          <Button onClick={() => answer(false)}>Cancel</Button>
          <Button variant="destructive" onClick={() => answer(true)}>
            Confirm
          </Button>
        </div>
      </Dialog>
    </Dialog.Root>
  );
}

export function ThemeControl() {
  const [mode, setMode] = useState(
    () => localStorage.getItem("flop_admin_theme") || "system",
  );
  useEffect(() => {
    const media = matchMedia("(prefers-color-scheme: dark)");
    const apply = () =>
      (document.documentElement.dataset.mode =
        mode === "system" ? (media.matches ? "dark" : "light") : mode);
    apply();
    media.addEventListener("change", apply);
    return () => media.removeEventListener("change", apply);
  }, [mode]);
  return (
    <Button
      variant="ghost"
      aria-label="Toggle color theme"
      onClick={() => {
        const next =
          document.documentElement.dataset.mode === "dark" ? "light" : "dark";
        localStorage.setItem("flop_admin_theme", next);
        setMode(next);
      }}
    >
      <SunIcon size={18} />
    </Button>
  );
}
function NavItem({ href, active, icon, children }) {
  const { setOpenMobile } = useSidebar();
  return (
    <Sidebar.MenuButton
      icon={icon}
      href={href}
      active={active}
      onClick={() => setOpenMobile(false)}
    >
      {children}
    </Sidebar.MenuButton>
  );
}
export function AppNavigation({
  tables,
  archiveTables,
  viewMode,
  currentTable,
  currentArchive,
  analyticsEnabled,
  mediaEnabled,
  observeEnabled,
  workflowEnabled,
  onLogout,
}) {
  const [search, setSearch] = useState("");
  const matching = tables.filter((t) =>
    t.name.toLowerCase().includes(search.toLowerCase()),
  );
  const { isMobile, openMobile } = useSidebar();
  const trapFocus = (event) => {
    if (!isMobile || !openMobile || event.key !== "Tab") return;
    const targets = [
      ...event.currentTarget.querySelectorAll("a[href],button,input"),
    ].filter((el) => !el.disabled && el.getClientRects().length);
    const first = targets[0],
      last = targets.at(-1);
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last?.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first?.focus();
    }
  };
  return (
    <Sidebar onKeyDown={trapFocus}>
      <Sidebar.Header>
        <a className="brand" href="#">
          <DatabaseIcon size={24} />
          <span>Flop</span>
          <Badge variant="secondary">Admin</Badge>
        </a>
      </Sidebar.Header>
      <Sidebar.Content>
        <Sidebar.Group>
          <Sidebar.GroupLabel>Workspace</Sidebar.GroupLabel>
          <Sidebar.Menu>
            <NavItem
              href="#"
              active={viewMode === "table" && !currentTable}
              icon={DatabaseIcon}
            >
              Overview
            </NavItem>
            {mediaEnabled && (
              <NavItem
                href="#_media"
                active={viewMode === "media"}
                icon={ImageIcon}
              >
                Media
              </NavItem>
            )}
          </Sidebar.Menu>
        </Sidebar.Group>
        <Sidebar.Group>
          <Sidebar.GroupLabel>
            Database <Badge variant="secondary">{tables.length}</Badge>
          </Sidebar.GroupLabel>
          <div className="nav-search">
            <Input
              aria-label="Find a table"
              placeholder="Find a table"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </div>
          <Sidebar.Menu>
            {matching.map((t) => (
              <NavItem
                key={t.name}
                href={"#" + encodeURIComponent(t.name)}
                active={
                  viewMode === "table" &&
                  !currentArchive &&
                  currentTable === t.name
                }
                icon={TableIcon}
              >
                <span className="nav-name">{t.name}</span>
                <Sidebar.MenuBadge>{t.rowCount}</Sidebar.MenuBadge>
              </NavItem>
            ))}
          </Sidebar.Menu>
          {!matching.length && (
            <p className="muted nav-search">No matching tables</p>
          )}
        </Sidebar.Group>
        {!!archiveTables.length && (
          <Sidebar.Group>
            <Sidebar.Menu>
              <Sidebar.MenuItem>
                <Sidebar.Collapsible defaultOpen={currentArchive}>
                  <Sidebar.CollapsibleTrigger
                    render={
                      <Sidebar.MenuButton icon={ArchiveIcon}>
                        Archive
                        <Sidebar.MenuChevron />
                      </Sidebar.MenuButton>
                    }
                  />
                  <Sidebar.CollapsibleContent>
                    <Sidebar.Menu>
                      {archiveTables.map((t) => (
                        <NavItem
                          key={t.name}
                          href={"#_archive/" + encodeURIComponent(t.name)}
                          active={currentArchive && currentTable === t.name}
                          icon={TableIcon}
                        >
                          {t.name}
                          <Sidebar.MenuBadge>{t.rowCount}</Sidebar.MenuBadge>
                        </NavItem>
                      ))}
                    </Sidebar.Menu>
                  </Sidebar.CollapsibleContent>
                </Sidebar.Collapsible>
              </Sidebar.MenuItem>
            </Sidebar.Menu>
          </Sidebar.Group>
        )}
        {(analyticsEnabled || observeEnabled) && (
          <Sidebar.Group>
            <Sidebar.GroupLabel>Monitor</Sidebar.GroupLabel>
            <Sidebar.Menu>
              {analyticsEnabled && (
                <>
                  <NavItem
                    href="#_analytics-volume"
                    active={viewMode === "analytics-volume"}
                    icon={ChartLineIcon}
                  >
                    Request volume
                  </NavItem>
                  <NavItem
                    href="#_analytics-performance"
                    active={viewMode === "analytics-performance"}
                    icon={ChartLineIcon}
                  >
                    Performance
                  </NavItem>
                </>
              )}
              {observeEnabled && (
                <NavItem
                  href="#_observe"
                  active={viewMode === "observe"}
                  icon={PulseIcon}
                >
                  Live activity
                </NavItem>
              )}
            </Sidebar.Menu>
          </Sidebar.Group>
        )}
        <Sidebar.Group>
          <Sidebar.GroupLabel>Manage</Sidebar.GroupLabel>
          <Sidebar.Menu>
            {workflowEnabled && (
              <NavItem
                href="#_workflow"
                active={viewMode === "workflow"}
                icon={FlowArrowIcon}
              >
                Workflows
              </NavItem>
            )}
            <NavItem
              href="#_mail"
              active={viewMode === "mail"}
              icon={EnvelopeIcon}
            >
              Mail
            </NavItem>
            <NavItem
              href="#_backups"
              active={viewMode === "backups"}
              icon={ClockCounterClockwiseIcon}
            >
              Backups
            </NavItem>
          </Sidebar.Menu>
        </Sidebar.Group>
      </Sidebar.Content>
      <Sidebar.Footer>
        <Sidebar.Menu>
          <Sidebar.MenuButton icon={SignOutIcon} onClick={onLogout}>
            Sign out
          </Sidebar.MenuButton>
        </Sidebar.Menu>
      </Sidebar.Footer>
    </Sidebar>
  );
}
export function PageFrame({ title, children }) {
  const { isMobile, openMobile } = useSidebar();
  return (
    <div className="workspace" inert={isMobile && openMobile}>
      <header className="workspace-bar">
        <div className="workspace-crumb">
          <Sidebar.Trigger aria-label="Open navigation" />
          <span className="muted">Flop</span>
          <span className="muted">/</span>
          <span>{title}</span>
        </div>
        <ThemeControl />
      </header>
      <main className="main">{children}</main>
    </div>
  );
}
export function TableLibrary({ tables, onSelect, loading, error, onRetry }) {
  const [query, setQuery] = useState("");
  const [scope, setScope] = useState("all");
  const matching = tables.filter(
    (t) =>
      (scope === "all" ||
        (scope === "system"
          ? t.name.startsWith("_")
          : !t.name.startsWith("_"))) &&
      t.name.toLowerCase().includes(query.toLowerCase()),
  );
  return (
    <section className="page library">
      <div className="page-heading">
        <div>
          <h1>Database overview</h1>
          <p className="muted">
            Browse your tables and manage the records in your database.
          </p>
        </div>
        <Badge variant="secondary">{tables.length} tables</Badge>
      </div>
      <div className="overview-metrics">
        <LayerCard className="metric-card">
          <span className="muted">Total records</span>
          <strong>
            {tables
              .reduce((n, t) => n + Number(t.rowCount || 0), 0)
              .toLocaleString()}
          </strong>
        </LayerCard>
        <LayerCard className="metric-card">
          <span className="muted">Application tables</span>
          <strong>
            {tables.filter((t) => !t.name.startsWith("_")).length}
          </strong>
        </LayerCard>
        <LayerCard className="metric-card">
          <span className="muted">System tables</span>
          <strong>{tables.filter((t) => t.name.startsWith("_")).length}</strong>
        </LayerCard>
      </div>
      <LayerCard className="library-card">
        <div className="library-controls">
          <h2>Tables</h2>
          <Input
            aria-label="Search table library"
            placeholder="Search tables"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
        </div>
        <div className="library-tabs">
          <Tabs
            value={scope}
            onValueChange={setScope}
            tabs={[
              { value: "all", label: "All tables" },
              { value: "application", label: "Application" },
              { value: "system", label: "System" },
            ]}
          />
        </div>
        {loading ? (
          <p role="status" className="empty-message">
            Loading tables…
          </p>
        ) : error ? (
          <div role="alert" className="empty-message">
            {error}
            <Button onClick={onRetry}>Try again</Button>
          </div>
        ) : (
          <Table>
            <Table.Header>
              <Table.Row>
                <Table.Head>Name</Table.Head>
                <Table.Head>Records</Table.Head>
                <Table.Head className="hide-small">Type</Table.Head>
                <Table.Head>
                  <span className="sr-only">Open table</span>
                </Table.Head>
              </Table.Row>
            </Table.Header>
            <Table.Body>
              {matching.map((t) => (
                <Table.Row key={t.name}>
                  <Table.Cell>
                    <Button
                      variant="ghost"
                      className="table-name"
                      onClick={() => onSelect(t.name)}
                    >
                      <TableIcon size={18} />
                      {t.name}
                    </Button>
                  </Table.Cell>
                  <Table.Cell>
                    {Number(t.rowCount || 0).toLocaleString()}
                  </Table.Cell>
                  <Table.Cell className="hide-small">
                    <Badge variant="secondary">
                      {t.materialized
                        ? "Materialized"
                        : t.name.startsWith("_")
                          ? "System"
                          : "Application"}
                    </Badge>
                  </Table.Cell>
                  <Table.Cell>
                    <Button
                      variant="ghost"
                      aria-label={"Open " + t.name}
                      onClick={() => onSelect(t.name)}
                    >
                      <ArrowRightIcon size={16} />
                    </Button>
                  </Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table>
        )}
        {!loading && !error && !matching.length && (
          <p className="empty-message">No tables match your search.</p>
        )}
      </LayerCard>
    </section>
  );
}

export function UploadButton({ children, className = "", ...props }) {
  return (
    <Button
      render={<label />}
      nativeButton={false}
      className={className}
      {...props}
    >
      {children}
    </Button>
  );
}
