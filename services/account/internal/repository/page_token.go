package repository

import (
	"encoding/base64"
	"encoding/json"
	"time"
)

type PageToken struct {
	LastCreatedAt time.Time `json:"last_created_at"`
	LastID        string    `json:"last_id"`
}

func EncodePageToken(lastCreatedAt time.Time, lastID string) (string, error) {
	token := PageToken{
		LastCreatedAt: lastCreatedAt,
		LastID:        lastID,
	}

	jsonBytes, err := json.Marshal(token)
	if err != nil {
		return "", err
	}

	return base64.URLEncoding.EncodeToString(jsonBytes), nil
}

func DecodePageToken(tokenStr string) (time.Time, string, error) {
	if tokenStr == "" {
		return time.Time{}, "", nil
	}

	decodedBytes, err := base64.URLEncoding.DecodeString(tokenStr)
	if err != nil {
		return time.Time{}, "", err
	}

	var token PageToken
	if err := json.Unmarshal(decodedBytes, &token); err != nil {
		return time.Time{}, "", err
	}

	return token.LastCreatedAt, token.LastID, nil
}
