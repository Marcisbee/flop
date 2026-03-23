package jsonstd

import stdjson "encoding/json"

func Marshal(v any) ([]byte, error) {
	return stdjson.Marshal(v)
}
