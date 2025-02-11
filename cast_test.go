package duckdb

type VarcharToTinyInt struct {
}

func (v VarcharToTinyInt) Config() CastFunctionConfig {
	return CastFunctionConfig{
		Source: "VARCHAR",
		Target: "TINYINT",
	}
}

func (v VarcharToTinyInt) Exec(ctx *CastExecContext) error {
	in := GetData[Varchar](ctx.Input())
	out := GetData[int8](ctx.Output())

	for i := range ctx.Count() {
		out[i] = int8(len(in[0].Bytes()))
	}
	return nil
}

func Check(err error) {
	if err != nil {
		panic(err)
	}
}
