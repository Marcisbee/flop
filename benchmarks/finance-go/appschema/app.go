package appschema

import (
	flop "github.com/marcisbee/flop"
	"github.com/marcisbee/flop/internal/schema"
)

func Build() *flop.App {
	app := flop.New(flop.Config{})

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

	return app
}

func BuildTableDefs() map[string]*schema.TableDef {
	return Build().BuildEngineTableDefs()
}
