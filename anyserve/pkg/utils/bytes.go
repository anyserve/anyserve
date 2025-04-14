package utils

import (
	"errors"
)

func ToBytes(v any) ([]byte, error) {
	switch val := v.(type) {
	case string:
		return []byte(val), nil
	case []byte:
		return val, nil
	default:
		return nil, errors.New("unsupported type")
	}
}
