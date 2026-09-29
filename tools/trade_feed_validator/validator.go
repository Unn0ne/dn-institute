package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"sort"
	"strings"
	"time"
)

var csvFields = []string{
	"event_id",
	"tx_hash",
	"block_time",
	"wallet",
	"side",
	"amount",
	"ingested_at",
}

var requiredFields = []string{"event_id", "tx_hash", "wallet", "side", "amount", "ingested_at"}

var decimalPattern = regexp.MustCompile(`^\+?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)

type IssueCode string

const (
	MissingRequiredField     IssueCode = "MISSING_REQUIRED_FIELD"
	MissingBlockTime         IssueCode = "MISSING_BLOCK_TIME"
	InvalidBlockTime         IssueCode = "INVALID_BLOCK_TIME"
	InvalidIngestedAt        IssueCode = "INVALID_INGESTED_AT"
	InvalidSide              IssueCode = "INVALID_SIDE"
	InvalidAmount            IssueCode = "INVALID_AMOUNT"
	IngestionBeforeBlockTime IssueCode = "INGESTION_BEFORE_BLOCK_TIME"
	DuplicateEventID         IssueCode = "DUPLICATE_EVENT_ID"
	DuplicateTrade           IssueCode = "DUPLICATE_TRADE"
	MixedTimestampFormat     IssueCode = "MIXED_TIMESTAMP_FORMAT"
	MalformedRow             IssueCode = "MALFORMED_ROW"
)

type ValidationIssue struct {
	Code           IssueCode `json:"code"`
	EventID        string    `json:"event_id"`
	SourceRow      int       `json:"source_row"`
	Message        string    `json:"message"`
	RelatedEventID string    `json:"related_event_id,omitempty"`
}

type TradeEvent struct {
	EventID    string
	TxHash     string
	BlockTime  time.Time
	Wallet     string
	Side       string
	Amount     big.Rat
	IngestedAt time.Time
	SourceRow  int
	Raw        map[string]string
}

type RejectedEvent struct {
	EventID   string
	SourceRow int
	Raw       map[string]string
	Issues    []ValidationIssue
}

type ValidationResult struct {
	InputRows int
	Columns   []string
	Accepted  []TradeEvent
	Rejected  []RejectedEvent
	RunDir    string
}

func (r ValidationResult) Issues() []ValidationIssue {
	count := 0
	for _, event := range r.Rejected {
		count += len(event.Issues)
	}

	issues := make([]ValidationIssue, 0, count)
	for _, event := range r.Rejected {
		issues = append(issues, event.Issues...)
	}
	return issues
}

func (r ValidationResult) IssueCounts() map[IssueCode]int {
	counts := make(map[IssueCode]int)
	for _, event := range r.Rejected {
		for _, issue := range event.Issues {
			counts[issue.Code]++
		}
	}
	return counts
}

type inputRow struct {
	values           map[string]string
	sourceRow        int
	malformedMessage string
}

type tradeFingerprint struct {
	txHash    string
	blockTime time.Time
	wallet    string
	side      string
	amount    string
}

type tradeOrigin struct {
	eventID   string
	sourceRow int
}

type timestampFormat uint8

const (
	unknownFormat timestampFormat = iota
	timeOnlyFormat
	rfc3339Format
)

func validateCSV(reader io.Reader) (ValidationResult, error) {
	csvReader := csv.NewReader(reader)
	csvReader.FieldsPerRecord = -1

	header, err := csvReader.Read()
	if err == io.EOF {
		return ValidationResult{}, fmt.Errorf("feed schema: CSV is empty")
	}
	if err != nil {
		return ValidationResult{}, fmt.Errorf("read CSV header: %w", err)
	}

	headerIndex := make(map[string]int, len(header))
	for index, name := range header {
		name = strings.TrimSpace(strings.TrimPrefix(name, "\uFEFF"))
		if name == "" {
			return ValidationResult{}, fmt.Errorf("feed schema: header column %d is empty", index+1)
		}
		if _, exists := headerIndex[name]; exists {
			return ValidationResult{}, fmt.Errorf("feed schema: duplicate CSV column %q", name)
		}
		if name == "source_row" || name == "validation_codes" || name == "validation_messages" {
			return ValidationResult{}, fmt.Errorf("feed schema: reserved output column %q", name)
		}
		header[index] = name
		headerIndex[name] = index
	}

	missingColumns := make([]string, 0)
	for _, field := range csvFields {
		if _, exists := headerIndex[field]; !exists {
			missingColumns = append(missingColumns, field)
		}
	}
	if len(missingColumns) > 0 {
		return ValidationResult{}, fmt.Errorf(
			"feed schema: missing required CSV column(s): %s",
			strings.Join(missingColumns, ", "),
		)
	}

	rows := make([]inputRow, 0)
	for recordNumber := 2; ; recordNumber++ {
		record, readErr := csvReader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return ValidationResult{}, fmt.Errorf("read CSV record %d: %w", recordNumber, readErr)
		}
		sourceRow := recordNumber
		if len(record) > 0 {
			sourceRow, _ = csvReader.FieldPos(0)
		}

		values := make(map[string]string, len(header))
		for index, field := range header {
			if index < len(record) {
				values[field] = record[index]
			} else {
				values[field] = ""
			}
		}

		row := inputRow{values: values, sourceRow: sourceRow}
		if len(record) != len(header) {
			row.malformedMessage = fmt.Sprintf(
				"row has %d value(s), but the CSV header defines %d",
				len(record),
				len(header),
			)
		}
		rows = append(rows, row)
	}

	return validateInputRows(rows, header), nil
}

func validateRows(rows []map[string]string, startRow int) ValidationResult {
	inputRows := make([]inputRow, 0, len(rows))
	for index, row := range rows {
		inputRows = append(inputRows, inputRow{
			values:    row,
			sourceRow: startRow + index,
		})
	}
	return validateInputRows(inputRows, csvFields)
}

func validateInputRows(rows []inputRow, columns []string) ValidationResult {
	result := ValidationResult{
		InputRows: len(rows),
		Columns:   append([]string(nil), columns...),
	}
	result.Accepted = make([]TradeEvent, 0, len(rows))
	result.Rejected = make([]RejectedEvent, 0)

	seenEventIDs := make(map[string]int, len(rows))
	seenTrades := make(map[tradeFingerprint]tradeOrigin, len(rows))
	feedTimeFormat := unknownFormat

	for _, incoming := range rows {
		raw := cloneRow(incoming.values)
		values := normaliseRow(raw)
		eventID := values["event_id"]
		if isMissing(eventID) {
			eventID = ""
		}
		displayID := eventID
		if displayID == "" {
			displayID = fmt.Sprintf("<row %d>", incoming.sourceRow)
		}

		issues := make([]ValidationIssue, 0, 2)
		addIssue := func(code IssueCode, message, relatedEventID string) {
			issues = append(issues, ValidationIssue{
				Code:           code,
				EventID:        displayID,
				SourceRow:      incoming.sourceRow,
				Message:        message,
				RelatedEventID: relatedEventID,
			})
		}

		if incoming.malformedMessage != "" {
			addIssue(MalformedRow, incoming.malformedMessage, "")
		}

		for _, field := range requiredFields {
			if isMissing(values[field]) {
				addIssue(MissingRequiredField, fmt.Sprintf("required field %q is missing", field), "")
			}
		}

		if isMissing(values["block_time"]) {
			addIssue(MissingBlockTime, "block_time is required for time-based analytics", "")
		}

		var blockTime time.Time
		blockTimeValid := false
		if !isMissing(values["block_time"]) {
			parsed, parseErr := parseUTCTimestamp(values["block_time"])
			if parseErr != nil {
				addIssue(InvalidBlockTime, fmt.Sprintf("block_time is invalid: %v", parseErr), "")
			} else {
				blockTime = parsed
				blockTimeValid = true
			}
		}

		var ingestedAt time.Time
		ingestedAtValid := false
		if !isMissing(values["ingested_at"]) {
			parsed, parseErr := parseUTCTimestamp(values["ingested_at"])
			if parseErr != nil {
				addIssue(InvalidIngestedAt, fmt.Sprintf("ingested_at is invalid: %v", parseErr), "")
			} else {
				ingestedAt = parsed
				ingestedAtValid = true
			}
		}

		side := strings.ToUpper(values["side"])
		sideValid := false
		if !isMissing(values["side"]) {
			if side != "BUY" && side != "SELL" {
				addIssue(InvalidSide, fmt.Sprintf("side must be BUY or SELL, got %q", values["side"]), "")
			} else {
				sideValid = true
			}
		}

		var amount *big.Rat
		amountValid := false
		if !isMissing(values["amount"]) {
			parsed, parseErr := parsePositiveDecimal(values["amount"])
			if parseErr != nil {
				addIssue(InvalidAmount, parseErr.Error(), "")
			} else {
				amount = parsed
				amountValid = true
			}
		}

		comparableTimes := blockTimeValid && ingestedAtValid
		if comparableTimes {
			blockFormat := timestampFormatOf(values["block_time"])
			ingestionFormat := timestampFormatOf(values["ingested_at"])
			if blockFormat != ingestionFormat {
				addIssue(MixedTimestampFormat, "block_time and ingested_at use different timestamp formats", "")
				comparableTimes = false
			} else if feedTimeFormat != unknownFormat && blockFormat != feedTimeFormat {
				addIssue(MixedTimestampFormat, "timestamp format differs from earlier accepted rows", "")
				comparableTimes = false
			}
		}

		if comparableTimes && ingestedAt.Before(blockTime) {
			addIssue(IngestionBeforeBlockTime, "ingested_at is earlier than block_time", "")
		}

		if eventID != "" {
			if firstRow, exists := seenEventIDs[eventID]; exists {
				addIssue(
					DuplicateEventID,
					fmt.Sprintf("event_id was already seen on source row %d", firstRow),
					eventID,
				)
			}
		}

		var fingerprint tradeFingerprint
		fingerprintValid := !isMissing(values["tx_hash"]) && !isMissing(values["wallet"]) &&
			comparableTimes && sideValid && amountValid
		if fingerprintValid {
			// Replays may get a new event ID and ingestion time, so neither belongs here.
			fingerprint = tradeFingerprint{
				txHash:    strings.ToLower(values["tx_hash"]),
				blockTime: blockTime,
				wallet:    strings.ToLower(values["wallet"]),
				side:      side,
				amount:    amount.RatString(),
			}
			if original, exists := seenTrades[fingerprint]; exists {
				addIssue(
					DuplicateTrade,
					fmt.Sprintf(
						"semantic trade duplicates %s from source row %d",
						original.eventID,
						original.sourceRow,
					),
					original.eventID,
				)
			}
		}

		if len(issues) > 0 {
			result.Rejected = append(result.Rejected, RejectedEvent{
				EventID:   displayID,
				SourceRow: incoming.sourceRow,
				Raw:       raw,
				Issues:    issues,
			})
			continue
		}

		if eventID == "" || isMissing(values["tx_hash"]) || isMissing(values["wallet"]) ||
			!blockTimeValid || !ingestedAtValid || !sideValid || !amountValid || !fingerprintValid {
			panic("validator invariant violated")
		}

		canonicalRaw := cloneRow(raw)
		for _, field := range csvFields {
			canonicalRaw[field] = values[field]
		}
		canonicalRaw["side"] = side
		event := TradeEvent{
			EventID:    eventID,
			TxHash:     values["tx_hash"],
			BlockTime:  blockTime,
			Wallet:     values["wallet"],
			Side:       side,
			Amount:     *new(big.Rat).Set(amount),
			IngestedAt: ingestedAt,
			SourceRow:  incoming.sourceRow,
			Raw:        canonicalRaw,
		}
		result.Accepted = append(result.Accepted, event)
		seenEventIDs[eventID] = incoming.sourceRow
		seenTrades[fingerprint] = tradeOrigin{eventID: event.EventID, sourceRow: event.SourceRow}
		if feedTimeFormat == unknownFormat {
			feedTimeFormat = timestampFormatOf(values["block_time"])
		}
	}

	sort.Slice(result.Accepted, func(i, j int) bool {
		left, right := result.Accepted[i], result.Accepted[j]
		if !left.BlockTime.Equal(right.BlockTime) {
			return left.BlockTime.Before(right.BlockTime)
		}
		if left.TxHash != right.TxHash {
			return left.TxHash < right.TxHash
		}
		return left.EventID < right.EventID
	})
	return result
}

func normaliseRow(row map[string]string) map[string]string {
	normalised := make(map[string]string, len(csvFields))
	for _, field := range csvFields {
		normalised[field] = strings.TrimSpace(row[field])
	}
	return normalised
}

func cloneRow(row map[string]string) map[string]string {
	cloned := make(map[string]string, len(row))
	for key, value := range row {
		cloned[key] = value
	}
	return cloned
}

func isMissing(value string) bool {
	return value == "" || strings.EqualFold(value, "null")
}

func timestampFormatOf(value string) timestampFormat {
	if strings.Contains(value, "T") || strings.Contains(value, "-") {
		return rfc3339Format
	}
	return timeOnlyFormat
}

func parsePositiveDecimal(value string) (*big.Rat, error) {
	if !decimalPattern.MatchString(value) {
		return nil, fmt.Errorf("amount must be a decimal number, got %q", value)
	}

	amount, ok := new(big.Rat).SetString(value)
	if !ok || amount.Sign() <= 0 {
		return nil, fmt.Errorf("amount must be a finite positive number, got %q", value)
	}
	return amount, nil
}

func parseUTCTimestamp(value string) (time.Time, error) {
	if timestampFormatOf(value) == rfc3339Format {
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return time.Time{}, fmt.Errorf("expected an RFC3339 UTC timestamp, got %q", value)
		}
		_, offset := parsed.Zone()
		if offset != 0 {
			return time.Time{}, fmt.Errorf("timestamp must be UTC, got %q", value)
		}
		return parsed.UTC(), nil
	}

	var parsed time.Time
	var err error
	if strings.HasSuffix(value, "Z") {
		parsed, err = time.Parse("15:04:05Z", value)
	} else {
		parsed, err = time.Parse("15:04:05", value)
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("expected HH:MM:SS in UTC, got %q", value)
	}
	// The challenge declares all time-only values to be on the same UTC day.
	return time.Date(
		1970,
		time.January,
		1,
		parsed.Hour(),
		parsed.Minute(),
		parsed.Second(),
		parsed.Nanosecond(),
		time.UTC,
	), nil
}
