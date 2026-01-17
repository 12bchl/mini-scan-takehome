package model

import (
	"encoding/json"
	"fmt"

	"github.com/censys/scan-takehome/pkg/scanning"
)

type ScanResult struct {
	scanBase
	Response string `json:"-"`
}

type scanRaw struct {
	scanBase
	DataVersion int             `json:"data_version"`
	Data        json.RawMessage `json:"data"`
}

type scanBase struct {
	Ip        string `json:"ip"`
	Port      uint32 `json:"port"`
	Service   string `json:"service"`
	Timestamp int64  `json:"timestamp"`
}

func (s *ScanResult) UnmarshalJSON(b []byte) error {
	var raw scanRaw
	if err := json.Unmarshal(b, &raw); err != nil {
		return fmt.Errorf("unmarshal scan: %w", err)
	}

	s.scanBase = raw.scanBase

	switch raw.DataVersion {
	case 1:
		var v1 scanning.V1Data
		if err := json.Unmarshal(raw.Data, &v1); err != nil {
			return fmt.Errorf("unmarshal v1 response: %w", err)
		}

		s.Response = string(v1.ResponseBytesUtf8)

	case 2:
		var v2 scanning.V2Data
		if err := json.Unmarshal(raw.Data, &v2); err != nil {
			return fmt.Errorf("unmarshal v2 response: %w", err)
		}
		s.Response = v2.ResponseStr

	default:
		return fmt.Errorf("unknown response format version %d", raw.DataVersion)
	}

	return nil
}
