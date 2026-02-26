// Page routing — route() builder, HeadConfig, pattern matching, SSR head rendering

// ---- HeadConfig ----

export interface HeadConfig {
  title?: string;
  charset?: string;
  viewport?: string;
  meta?: Array<Record<string, string>>;
  og?: Record<string, string>;
  twitter?: Record<string, string>;
  link?: Array<Record<string, string>>;
  script?: Array<{ src?: string; type?: string; async?: boolean; defer?: boolean; content?: string; [key: string]: string | boolean | undefined }>;
  style?: Array<{ content: string; media?: string }>;
  base?: { href?: string; target?: string };
  noscript?: string;
}

// ---- PageContext (passed to head() functions) ----

export interface PageContext {
  params: Record<string, string>;
  api: Record<string, (params?: any) => Promise<any>>;
}

export type HeadFn = (ctx: PageContext) => HeadConfig | Promise<HeadConfig>;

// ---- RouteNode (what route() returns) ----

export interface RouteNode {
  _type: "route";
  pattern: string;
  head: HeadFn | null;
  component: (() => Promise<any>) | null;
  children: RouteNode[];
}

// ---- route() builder ----

export interface RouteConfig {
  head?: HeadFn;
  component?: () => Promise<any>;
  children?: RouteNode[];
}

export function route(pattern: string, config?: RouteConfig, children?: RouteNode[]): RouteNode {
  return {
    _type: "route",
    pattern,
    head: config?.head ?? null,
    component: config?.component ?? null,
    children: children ?? config?.children ?? [],
  };
}

// ---- FlatPageRoute (flattened for engine matching) ----

export interface FlatPageRoute {
  pattern: string;
  regex: RegExp;
  paramNames: string[];
  headChain: HeadFn[];
  componentPaths: string[];
}

// ---- Extract import path from component function ----

export function extractImportPath(fn: (() => Promise<any>) | null): string | null {
  if (!fn) return null;
  const src = fn.toString();
  const match = src.match(/import\s*\(\s*["']([^"']+)["']\s*\)/);
  return match ? match[1] : null;
}

// ---- Flatten route tree ----

export function flattenRoutes(
  node: RouteNode,
  parentPattern = "",
  parentHeadChain: HeadFn[] = [],
  parentComponentPaths: string[] = [],
): FlatPageRoute[] {
  const results: FlatPageRoute[] = [];

  // Combine parent + current pattern
  const fullPattern = joinPatterns(parentPattern, node.pattern);
  const headChain = node.head ? [...parentHeadChain, node.head] : [...parentHeadChain];

  // Extract component import path from function source
  const componentPath = extractImportPath(node.component);
  const componentPaths = componentPath
    ? [...parentComponentPaths, componentPath]
    : [...parentComponentPaths];

  if (node.children.length === 0) {
    // Leaf route
    const { regex, paramNames } = compilePattern(fullPattern);
    results.push({ pattern: fullPattern, regex, paramNames, headChain, componentPaths });
  } else {
    // Has children — flatten each child
    for (const child of node.children) {
      results.push(...flattenRoutes(child, fullPattern, headChain, componentPaths));
    }
  }

  return results;
}

function joinPatterns(parent: string, child: string): string {
  if (parent === "/" && child === "/") return "/";
  if (child === "/") return parent || "/";
  const base = parent === "/" ? "" : parent;
  return base + child;
}

// ---- Pattern matching ----

export function compilePattern(pattern: string): { regex: RegExp; paramNames: string[] } {
  const paramNames: string[] = [];
  const regexStr = pattern
    .split("/")
    .map((segment) => {
      if (segment.startsWith(":")) {
        paramNames.push(segment.slice(1));
        return "([^/]+)";
      }
      if (segment === "*") {
        paramNames.push("*");
        return "(.*)";
      }
      return escapeRegex(segment);
    })
    .join("/");

  return { regex: new RegExp(`^${regexStr}$`), paramNames };
}

function escapeRegex(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function matchPageRoute(
  pathname: string,
  routes: FlatPageRoute[],
): { route: FlatPageRoute; params: Record<string, string> } | null {
  for (const route of routes) {
    const match = route.regex.exec(pathname);
    if (match) {
      const params: Record<string, string> = {};
      for (let i = 0; i < route.paramNames.length; i++) {
        params[route.paramNames[i]] = match[i + 1];
      }
      return { route, params };
    }
  }
  return null;
}

// ---- Head merging ----

export function mergeHeadConfigs(configs: HeadConfig[]): HeadConfig {
  const merged: HeadConfig = {};

  for (const cfg of configs) {
    // Scalars: child overrides parent
    if (cfg.title !== undefined) merged.title = cfg.title;
    if (cfg.charset !== undefined) merged.charset = cfg.charset;
    if (cfg.viewport !== undefined) merged.viewport = cfg.viewport;
    if (cfg.base !== undefined) merged.base = cfg.base;
    if (cfg.noscript !== undefined) merged.noscript = cfg.noscript;

    // Arrays: concatenate
    if (cfg.meta) merged.meta = [...(merged.meta ?? []), ...cfg.meta];
    if (cfg.link) merged.link = [...(merged.link ?? []), ...cfg.link];
    if (cfg.script) merged.script = [...(merged.script ?? []), ...cfg.script];
    if (cfg.style) merged.style = [...(merged.style ?? []), ...cfg.style];

    // Object shorthands: merge keys (child overrides parent per-key)
    if (cfg.og) merged.og = { ...(merged.og ?? {}), ...cfg.og };
    if (cfg.twitter) merged.twitter = { ...(merged.twitter ?? {}), ...cfg.twitter };
  }

  return merged;
}

// ---- Head rendering (HeadConfig → HTML string) ----

export function renderHeadConfig(config: HeadConfig): string {
  const lines: string[] = [];

  if (config.charset) {
    lines.push(`<meta charset="${esc(config.charset)}">`);
  }

  if (config.base) {
    const attrs = Object.entries(config.base)
      .filter(([_, v]) => v !== undefined)
      .map(([k, v]) => `${k}="${esc(v!)}"`)
      .join(" ");
    lines.push(`<base ${attrs}>`);
  }

  if (config.title) {
    lines.push(`<title>${esc(config.title)}</title>`);
  }

  if (config.viewport) {
    lines.push(`<meta name="viewport" content="${esc(config.viewport)}">`);
  }

  // <meta> tags
  if (config.meta) {
    for (const meta of config.meta) {
      const attrs = Object.entries(meta)
        .filter(([_, v]) => v !== undefined)
        .map(([k, v]) => `${k}="${esc(v)}"`)
        .join(" ");
      lines.push(`<meta ${attrs}>`);
    }
  }

  // og: shorthand → <meta property="og:*">
  if (config.og) {
    for (const [key, value] of Object.entries(config.og)) {
      if (value !== undefined) {
        lines.push(`<meta property="og:${esc(key)}" content="${esc(value)}">`);
      }
    }
  }

  // twitter: shorthand → <meta name="twitter:*">
  if (config.twitter) {
    for (const [key, value] of Object.entries(config.twitter)) {
      if (value !== undefined) {
        lines.push(`<meta name="twitter:${esc(key)}" content="${esc(value)}">`);
      }
    }
  }

  // <link> tags
  if (config.link) {
    for (const link of config.link) {
      const attrs = Object.entries(link)
        .filter(([_, v]) => v !== undefined)
        .map(([k, v]) => `${k}="${esc(v)}"`)
        .join(" ");
      lines.push(`<link ${attrs}>`);
    }
  }

  // <style> tags
  if (config.style) {
    for (const style of config.style) {
      const mediaAttr = style.media ? ` media="${esc(style.media)}"` : "";
      lines.push(`<style${mediaAttr}>${style.content}</style>`);
    }
  }

  // <script> tags in head
  if (config.script) {
    for (const script of config.script) {
      const { content, src, async: isAsync, defer, ...rest } = script;
      const attrs: string[] = [];
      if (src) attrs.push(`src="${esc(src)}"`);
      if (isAsync) attrs.push("async");
      if (defer) attrs.push("defer");
      for (const [k, v] of Object.entries(rest)) {
        if (typeof v === "string") attrs.push(`${k}="${esc(v)}"`);
        else if (v === true) attrs.push(k);
      }
      const attrStr = attrs.length > 0 ? " " + attrs.join(" ") : "";
      if (content) {
        lines.push(`<script${attrStr}>${content}</script>`);
      } else {
        lines.push(`<script${attrStr}></script>`);
      }
    }
  }

  // <noscript> in head
  if (config.noscript) {
    lines.push(`<noscript>${config.noscript}</noscript>`);
  }

  return lines.join("\n");
}

function esc(str: string): string {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}
