// Client bundler — generates a virtual entry point from the route tree and bundles with esbuild

import * as esbuild from "npm:esbuild@0.24";
import { resolve, join } from "jsr:@std/path@1";
import type { RouteNode, FlatPageRoute } from "./route.ts";
import { extractImportPath } from "./route.ts";

// ---- Collect unique component paths from route tree ----

function collectComponents(
  node: RouteNode,
  appDir: string,
  seen: Map<string, number>,
  ordered: string[],
): void {
  const rel = extractImportPath(node.component);
  if (rel) {
    const abs = resolve(appDir, rel);
    if (!seen.has(abs)) {
      seen.set(abs, ordered.length);
      ordered.push(abs);
    }
  }
  for (const child of node.children) {
    collectComponents(child, appDir, seen, ordered);
  }
}

// ---- Generate virtual entry point ----

function generateEntryPoint(
  pageRoutes: FlatPageRoute[],
  routeTree: RouteNode,
  appDir: string,
): { code: string; resolveDir: string } {
  const pathToIndex = new Map<string, number>();
  const orderedPaths: string[] = [];
  collectComponents(routeTree, appDir, pathToIndex, orderedPaths);

  // Import statements
  const imports = orderedPaths.map(
    (p, i) => `import C${i} from ${JSON.stringify(p)};`,
  );

  // Route manifest — pattern + component index chain
  const manifest = pageRoutes.map((route) => {
    const ci = route.componentPaths
      .map((rel) => {
        const abs = resolve(appDir, rel);
        return pathToIndex.get(abs) ?? -1;
      })
      .filter((i) => i >= 0);
    return { pattern: route.pattern, ci };
  });

  const code = `import{createElement as h,useState,useEffect}from"react";
import{createRoot}from"react-dom/client";
${imports.join("\n")}
var cs=[${orderedPaths.map((_, i) => `C${i}`).join(",")}];
var routes=${JSON.stringify(manifest)};
${SPA_ROUTER}
createRoot(document.getElementById("root")).render(h(R,{routes:routes,cs:cs}));
`;

  return { code, resolveDir: appDir };
}

// ---- Minimal SPA router (embedded in bundle) ----

const SPA_ROUTER = `
function mp(p){
  var pn=[];
  var rs=p.split("/").map(function(s){
    if(s[0]===":"){pn.push(s.slice(1));return"([^/]+)"}
    if(s==="*"){pn.push("*");return"(.*)"}
    return s.replace(/[.*+?^\${}()|[\\]\\\\]/g,"\\\\$&")
  }).join("/");
  return{re:new RegExp("^"+rs+"$"),pn:pn}
}
function mr(path,routes){
  for(var i=0;i<routes.length;i++){
    var r=routes[i];
    var c=mp(r.pattern);
    var m=c.re.exec(path);
    if(m){
      var params={};
      for(var j=0;j<c.pn.length;j++)params[c.pn[j]]=m[j+1];
      return{route:r,params:params}
    }
  }
  return null
}
function nav(to){history.pushState(null,"",to);window.dispatchEvent(new PopStateEvent("popstate"))}
function R(props){
  var _s=useState(window.location.pathname),path=_s[0],setPath=_s[1];
  useEffect(function(){
    var fn=function(){setPath(window.location.pathname)};
    window.addEventListener("popstate",fn);
    return function(){window.removeEventListener("popstate",fn)}
  },[]);
  useEffect(function(){
    var fn=function(e){
      var a=e.target&&e.target.closest?e.target.closest("a"):null;
      if(!a||!a.href||a.target==="_blank"||a.hasAttribute("download"))return;
      if(e.ctrlKey||e.metaKey||e.shiftKey||e.altKey||e.button!==0)return;
      try{var u=new URL(a.href);if(u.origin!==window.location.origin)return;
        e.preventDefault();if(u.pathname!==window.location.pathname)nav(u.pathname)
      }catch(ex){}
    };
    document.addEventListener("click",fn);
    return function(){document.removeEventListener("click",fn)}
  },[]);
  var result=mr(path,props.routes);
  if(!result)return h("div",null,"404 - Not found");
  var r=result.route,params=result.params;
  var el=null;
  for(var i=r.ci.length-1;i>=0;i--){
    var Comp=props.cs[r.ci[i]];
    el=i===r.ci.length-1?h(Comp,{params:params}):h(Comp,{children:el})
  }
  return el
}
`;

// ---- Find Deno's npm cache path ----

async function getNpmCachePath(): Promise<string | null> {
  try {
    const cmd = new Deno.Command("deno", {
      args: ["info", "--json"],
      stdout: "piped",
      stderr: "piped",
    });
    const output = await cmd.output();
    if (output.success) {
      const info = JSON.parse(new TextDecoder().decode(output.stdout));
      return info.npmCache ?? null;
    }
  } catch {
    // Fall through
  }
  return null;
}

// ---- Find package version in npm cache ----

function findPackageInCache(npmCache: string, packageName: string): string | null {
  const pkgDir = join(npmCache, "registry.npmjs.org", packageName);
  try {
    // Find the highest version directory
    const entries: string[] = [];
    for (const entry of Deno.readDirSync(pkgDir)) {
      if (entry.isDirectory) entries.push(entry.name);
    }
    if (entries.length === 0) return null;
    // Sort semver-ish and pick latest
    entries.sort((a, b) => {
      const ap = a.split(".").map(Number);
      const bp = b.split(".").map(Number);
      for (let i = 0; i < 3; i++) {
        if ((ap[i] || 0) !== (bp[i] || 0)) return (bp[i] || 0) - (ap[i] || 0);
      }
      return 0;
    });
    return join(pkgDir, entries[0]);
  } catch {
    return null;
  }
}

// ---- esbuild plugin to resolve npm packages from Deno cache ----

function denoNpmPlugin(npmCache: string): esbuild.Plugin {
  return {
    name: "deno-npm",
    setup(build) {
      // Resolve any bare specifier that isn't a relative/absolute path
      build.onResolve({ filter: /^[^./]/ }, (args) => {
        // Split package name from subpath: "react-dom/client" → ["react-dom", "client"]
        // Handle scoped packages too: "@scope/pkg/sub" → ["@scope/pkg", "sub"]
        const parts = args.path.split("/");
        let pkgName: string;
        let subpath: string;

        if (parts[0].startsWith("@")) {
          pkgName = parts.slice(0, 2).join("/");
          subpath = parts.slice(2).join("/");
        } else {
          pkgName = parts[0];
          subpath = parts.slice(1).join("/");
        }

        const pkgDir = findPackageInCache(npmCache, pkgName);
        if (!pkgDir) return undefined;

        if (subpath) {
          // Try exact file, then with .js extension, then as directory with index.js
          const candidates = [
            join(pkgDir, subpath),
            join(pkgDir, subpath + ".js"),
            join(pkgDir, subpath, "index.js"),
          ];
          for (const candidate of candidates) {
            try {
              Deno.statSync(candidate);
              return { path: candidate };
            } catch {
              continue;
            }
          }
        }

        // Package root — read package.json to find main/module entry
        try {
          const pkgJson = JSON.parse(Deno.readTextFileSync(join(pkgDir, "package.json")));
          const entry = pkgJson.module || pkgJson.main || "index.js";
          return { path: join(pkgDir, entry) };
        } catch {
          return { path: join(pkgDir, "index.js") };
        }
      });
    },
  };
}

// ---- Bundle pages ----

export async function bundlePages(
  routeTree: RouteNode,
  pageRoutes: FlatPageRoute[],
  appDir: string,
): Promise<{ js: Uint8Array; css: Uint8Array; errors: string[] }> {
  const { code, resolveDir } = generateEntryPoint(pageRoutes, routeTree, appDir);

  // Find Deno's npm cache for resolving react/react-dom
  const npmCache = await getNpmCachePath();
  if (!npmCache) {
    return {
      js: new Uint8Array(),
      css: new Uint8Array(),
      errors: ["Could not locate Deno npm cache. Run 'deno cache' first."],
    };
  }

  const plugins: esbuild.Plugin[] = [denoNpmPlugin(npmCache)];

  try {
    const result = await esbuild.build({
      stdin: {
        contents: code,
        loader: "tsx",
        resolveDir,
        sourcefile: "flop_client_entry.tsx",
      },
      bundle: true,
      format: "esm",
      target: ["es2020"],
      platform: "browser",
      minify: true,
      sourcemap: false,
      jsx: "automatic",
      jsxImportSource: "react",
      write: false,
      outdir: "out",
      plugins,
    });

    await esbuild.stop();

    const jsFile = result.outputFiles?.find((f) => f.path.endsWith(".js"));
    const cssFile = result.outputFiles?.find((f) => f.path.endsWith(".css"));

    return {
      js: jsFile?.contents ?? new Uint8Array(),
      css: cssFile?.contents ?? new Uint8Array(),
      errors: result.errors.map((e) => e.text),
    };
  } catch (err) {
    try { await esbuild.stop(); } catch { /* ignore */ }
    return {
      js: new Uint8Array(),
      css: new Uint8Array(),
      errors: [err instanceof Error ? err.message : String(err)],
    };
  }
}
