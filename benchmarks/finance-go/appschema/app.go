package appschema

import (
	flop "github.com/marcisbee/flop"
	"github.com/marcisbee/flop/internal/schema"
)

type BuildOptions struct {
	DataDir  string
	SyncMode string
}

func Build() *flop.App {
	return BuildWithOptions(BuildOptions{})
}

func BuildWithOptions(opts BuildOptions) *flop.App {
	cfg := flop.Config{
		DataDir:               opts.DataDir,
		SyncMode:              opts.SyncMode,
		AsyncSecondaryIndexes: true,
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "./data"
	}
	if cfg.SyncMode == "" {
		cfg.SyncMode = "normal"
	}

	app := flop.New(cfg)

	users := flop.Define(app, "users", func(s *flop.SchemaBuilder) {
		s.String("id").Primary("uuidv7").Required().Unique()
		s.String("email").Required().Unique().Email().MaxLen(255)
		s.Bcrypt("password", 10).Required()
		s.String("name").Required().MinLen(2).MaxLen(120)
		s.Roles("roles")
	})

	accounts := flop.Define(app, "accounts", func(s *flop.SchemaBuilder) {
		s.String("id").Primary("uuidv7").Required().Unique()
		s.Ref("ownerId", users, "id").Required().Index()
		s.String("name").Required().MaxLen(120)
		s.Enum("type", "checking", "savings", "credit").Required()
		s.Number("balance").Required()
		s.String("currency").Required().MaxLen(8)
		s.Timestamp("createdAt").DefaultNow()
	})

	transactions := flop.Define(app, "transactions", func(s *flop.SchemaBuilder) {
		s.String("id").Primary("uuidv7").Required().Unique()
		s.Ref("fromAccountId", accounts, "id").Required().Index()
		s.Ref("toAccountId", accounts, "id").Required().Index()
		s.Number("amount").Required()
		s.String("currency").Required().MaxLen(8)
		s.Enum("status", "pending", "completed", "failed").Required()
		s.String("description")
		s.Timestamp("createdAt").DefaultNow()
	})

	flop.Define(app, "ledger", func(s *flop.SchemaBuilder) {
		s.String("id").Primary("uuidv7").Required().Unique()
		s.Ref("accountId", accounts, "id").Required().Index()
		s.Ref("transactionId", transactions, "id").Required()
		s.Number("amount").Required()
		s.Number("balanceAfter").Required()
		s.Enum("type", "debit", "credit").Required()
		s.Timestamp("createdAt").DefaultNow()
	})

	flop.Define(app, "benchmark_stats", func(s *flop.SchemaBuilder) {
		s.String("id").Primary().Required().Unique()
		s.Integer("accountCount").Required().Default(0)
		s.Integer("transactionCount").Required().Default(0)
		s.Integer("completedTransactions").Required().Default(0)
		s.Integer("failedTransactions").Required().Default(0)
		s.Number("totalVolume").Required().Default(0)
		s.Number("totalBalance").Required().Default(0)
	})

	RegisterEndpoints(app)

	return app
}

func BuildTableDefs() map[string]*schema.TableDef {
	return Build().BuildEngineTableDefs()
}
