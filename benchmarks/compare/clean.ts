import { fromFileUrl, resolve } from "@std/path";

const ROOT = resolve(fromFileUrl(new URL(".", import.meta.url)), "..", "..");
const RESULTS_DIR = resolve(ROOT, "benchmarks", "compare", "results");
const TARGETS = [
  "history.json",
  "latest.json",
  "report.html",
  "runs",
  "tmp",
] as const;
const EXTRA_DATA_DIRS = [
  resolve(ROOT, "benchmarks", "finance-mongodb", "data"),
  resolve(ROOT, "benchmarks", "finance-mongodb-go", "data"),
] as const;

async function safeRemove(path: string) {
  try {
    await Deno.remove(path, { recursive: true });
    console.log(`removed ${path}`);
  } catch (err) {
    if (err instanceof Deno.errors.NotFound) return;
    throw err;
  }
}

async function main() {
  for (const target of TARGETS) {
    await safeRemove(resolve(RESULTS_DIR, target));
  }
  for (const dir of EXTRA_DATA_DIRS) {
    await safeRemove(dir);
  }

  await Deno.mkdir(resolve(RESULTS_DIR, "runs"), { recursive: true });
  await Deno.mkdir(resolve(RESULTS_DIR, "tmp"), { recursive: true });
  console.log(`cleaned benchmark results in ${RESULTS_DIR}`);
}

if (import.meta.main) {
  main().catch((err) => {
    console.error(err);
    Deno.exit(1);
  });
}
