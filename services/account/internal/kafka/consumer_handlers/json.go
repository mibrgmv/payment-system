package consumer_handlers

import (
	"bytes"
	"encoding/json"
)

func strictUnmarshal(data []byte, v interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(v)
}
