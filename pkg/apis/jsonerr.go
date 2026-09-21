package apis

import (
	"encoding/json"
	"errors"
	"strings"
)

func asType(err error, target **json.UnmarshalTypeError) bool {
	return errors.As(err, target)
}

func asSyntax(err error, target **json.SyntaxError) bool {
	return errors.As(err, target)
}

func isUnknownField(err error) bool {
	return err != nil && strings.Contains(err.Error(), "unknown field")
}

func stripJSONPrefix(err error) string {
	return strings.TrimPrefix(err.Error(), "json: ")
}
