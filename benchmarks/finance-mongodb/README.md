# MongoDB Finance Benchmark

Finance benchmark server backed by MongoDB.

Download local `mongod` binary into this repo (gitignored):

```bash
./benchmarks/finance-mongodb/download-mongod.sh
```

After this, benchmarks auto-detect `benchmarks/.tools/mongodb/mongod`.
`MONGOD_BIN` is optional (only if you want to override the binary path).

Run:

```bash
deno run --allow-all benchmarks/finance-mongodb/app.ts --port=1992
```

By default this auto-starts `mongod` (must be in `PATH`). You can also point to
an existing MongoDB instance:

```bash
deno run --allow-all benchmarks/finance-mongodb/app.ts --port=1992 --mongo-uri=mongodb://127.0.0.1:27017
```
