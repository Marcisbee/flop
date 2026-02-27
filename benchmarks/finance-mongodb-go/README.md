# MongoDB Go Finance Benchmark

Finance benchmark server backed by MongoDB (Go).

Run:

```bash
go run ./benchmarks/finance-mongodb-go --port=1998
```

By default this auto-starts `mongod` (must be in `PATH`). You can also point to
an existing MongoDB instance:

```bash
go run ./benchmarks/finance-mongodb-go --port=1998 --mongo-uri=mongodb://127.0.0.1:27017
```
