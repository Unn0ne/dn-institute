package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSampleFeedAcceptsOnlyCleanUniqueTrades(t *testing.T) {
	result := loadSampleFeed(t)

	if result.InputRows != 8 {
		t.Fatalf("InputRows = %d, want 8", result.InputRows)
	}
	if len(result.Rejected) != 4 {
		t.Fatalf("rejected rows = %d, want 4", len(result.Rejected))
	}

	gotIDs := make([]string, 0, len(result.Accepted))
	total := new(big.Rat)
	for index := range result.Accepted {
		event := &result.Accepted[index]
		gotIDs = append(gotIDs, event.EventID)
		total.Add(total, &event.Amount)
	}
	wantIDs := []string{"evt_001", "evt_002", "evt_004", "evt_006"}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("accepted IDs = %v, want %v", gotIDs, wantIDs)
	}
	if total.Cmp(big.NewRat(375_000, 1)) != 0 {
		t.Fatalf("accepted volume = %s, want 375000", total.RatString())
	}
}

func TestSampleFeedDetectsDelayedReplay(t *testing.T) {
	event := rejectedEvent(t, loadSampleFeed(t), "evt_003")
	assertOnlyIssue(t, event, DuplicateTrade)
	if event.Issues[0].RelatedEventID != "evt_002" {
		t.Fatalf("related event = %q, want evt_002", event.Issues[0].RelatedEventID)
	}
}

func TestSampleFeedDetectsSimultaneousReplay(t *testing.T) {
	event := rejectedEvent(t, loadSampleFeed(t), "evt_007")
	assertOnlyIssue(t, event, DuplicateTrade)
	if event.Issues[0].RelatedEventID != "evt_006" {
		t.Fatalf("related event = %q, want evt_006", event.Issues[0].RelatedEventID)
	}
}

func TestSampleFeedSendsMissingBlockTimeToDeadLetter(t *testing.T) {
	event := rejectedEvent(t, loadSampleFeed(t), "evt_005")
	assertOnlyIssue(t, event, MissingBlockTime)
}

func TestSampleFeedRejectsImpossibleIngestionTime(t *testing.T) {
	event := rejectedEvent(t, loadSampleFeed(t), "evt_008")
	assertOnlyIssue(t, event, IngestionBeforeBlockTime)
}

func TestSampleFeedIssueCounts(t *testing.T) {
	got := loadSampleFeed(t).IssueCounts()
	want := map[IssueCode]int{
		DuplicateTrade:           2,
		IngestionBeforeBlockTime: 1,
		MissingBlockTime:         1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("issue counts = %v, want %v", got, want)
	}
}

func TestPipelineWritesAnalyticsDeadLetterAndJSONReport(t *testing.T) {
	outputDirectory := t.TempDir()
	result, err := runPipeline("sample_feed.csv", outputDirectory)
	if err != nil {
		t.Fatalf("runPipeline() error = %v", err)
	}

	validRows := readCSVFile(t, filepath.Join(outputDirectory, validOutputName))
	if len(validRows) != 5 {
		t.Fatalf("valid CSV records including header = %d, want 5", len(validRows))
	}
	gotValidIDs := []string{validRows[1][0], validRows[2][0], validRows[3][0], validRows[4][0]}
	wantValidIDs := []string{"evt_001", "evt_002", "evt_004", "evt_006"}
	if !reflect.DeepEqual(gotValidIDs, wantValidIDs) {
		t.Fatalf("valid output IDs = %v, want %v", gotValidIDs, wantValidIDs)
	}

	rejectedRows := readCSVFile(t, filepath.Join(outputDirectory, rejectedOutputName))
	if len(rejectedRows) != 5 {
		t.Fatalf("rejected CSV records including header = %d, want 5", len(rejectedRows))
	}
	if rejectedRows[1][len(csvFields)+1] != string(DuplicateTrade) {
		t.Fatalf("first rejection code = %q, want %s", rejectedRows[1][len(csvFields)+1], DuplicateTrade)
	}

	reportFile, err := os.Open(filepath.Join(outputDirectory, reportOutputName))
	if err != nil {
		t.Fatalf("open report: %v", err)
	}
	t.Cleanup(func() {
		if err := reportFile.Close(); err != nil {
			t.Errorf("close report: %v", err)
		}
	})
	var report validationReport
	if err := json.NewDecoder(reportFile).Decode(&report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if report.Summary.AcceptedRows != len(result.Accepted) ||
		report.Summary.RejectedRows != len(result.Rejected) {
		t.Fatalf("report summary = %+v, result accepted/rejected = %d/%d",
			report.Summary, len(result.Accepted), len(result.Rejected))
	}
}

func TestPipelineErrorPaths(t *testing.T) {
	t.Run("missing input", func(t *testing.T) {
		_, err := runPipeline(filepath.Join(t.TempDir(), "missing.csv"), t.TempDir())
		if err == nil || !strings.Contains(err.Error(), "open input feed") {
			t.Fatalf("error = %v, want open input feed error", err)
		}
	})

	t.Run("output path is a file", func(t *testing.T) {
		outputPath := filepath.Join(t.TempDir(), "output-file")
		if err := os.WriteFile(outputPath, []byte("not a directory"), 0o600); err != nil {
			t.Fatalf("create output file: %v", err)
		}
		_, err := runPipeline("sample_feed.csv", outputPath)
		if err == nil || !strings.Contains(err.Error(), "create output directory") {
			t.Fatalf("error = %v, want create output directory error", err)
		}
	})

	t.Run("output artifact cannot be replaced", func(t *testing.T) {
		outputDirectory := t.TempDir()
		if err := os.Mkdir(filepath.Join(outputDirectory, validOutputName), 0o755); err != nil {
			t.Fatalf("create conflicting output directory: %v", err)
		}
		_, err := runPipeline("sample_feed.csv", outputDirectory)
		if err == nil || !strings.Contains(err.Error(), "write "+validOutputName) {
			t.Fatalf("error = %v, want valid output write error", err)
		}
	})
}

func TestOutputWritersPropagateErrors(t *testing.T) {
	result := loadSampleFeed(t)
	largeValue := strings.Repeat("x", 8*1024)

	validResult := result
	validResult.Accepted = append([]TradeEvent(nil), result.Accepted...)
	validResult.Accepted[0].Raw = cloneRow(validResult.Accepted[0].Raw)
	validResult.Accepted[0].Raw["wallet"] = largeValue
	if err := writeValidEvents(failingWriter{}, validResult); err == nil {
		t.Fatal("writeValidEvents() error = nil, want writer failure")
	}

	rejectedResult := result
	rejectedResult.Rejected = append([]RejectedEvent(nil), result.Rejected...)
	rejectedResult.Rejected[0].Raw = cloneRow(rejectedResult.Rejected[0].Raw)
	rejectedResult.Rejected[0].Raw["wallet"] = largeValue
	if err := writeRejectedEvents(failingWriter{}, rejectedResult); err == nil {
		t.Fatal("writeRejectedEvents() error = nil, want writer failure")
	}
}

func TestWriteAtomicallyErrorPaths(t *testing.T) {
	t.Run("temporary file creation", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing", "result.csv")
		err := writeAtomically(path, func(io.Writer) error { return nil })
		if err == nil {
			t.Fatal("writeAtomically() error = nil, want temporary file creation error")
		}
	})

	t.Run("writer callback", func(t *testing.T) {
		wanted := errors.New("injected write failure")
		path := filepath.Join(t.TempDir(), "result.csv")
		err := writeAtomically(path, func(io.Writer) error { return wanted })
		if !errors.Is(err, wanted) {
			t.Fatalf("error = %v, want %v", err, wanted)
		}
		if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("output stat error = %v, want not-exist", statErr)
		}
	})
}

func TestDefensiveDomainValidation(t *testing.T) {
	tests := []struct {
		name       string
		field      string
		value      string
		wantedCode IssueCode
	}{
		{name: "missing wallet", field: "wallet", value: "null", wantedCode: MissingRequiredField},
		{name: "invalid side", field: "side", value: "HOLD", wantedCode: InvalidSide},
		{name: "zero amount", field: "amount", value: "0", wantedCode: InvalidAmount},
		{name: "negative amount", field: "amount", value: "-1", wantedCode: InvalidAmount},
		{name: "non-finite amount", field: "amount", value: "NaN", wantedCode: InvalidAmount},
		{name: "fraction syntax", field: "amount", value: "1/2", wantedCode: InvalidAmount},
		{name: "invalid block time", field: "block_time", value: "25:00:00", wantedCode: InvalidBlockTime},
		{name: "invalid ingestion time", field: "ingested_at", value: "not-a-time", wantedCode: InvalidIngestedAt},
		{name: "non-UTC block time", field: "block_time", value: "2026-09-28T10:00:00+03:00", wantedCode: InvalidBlockTime},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			row := validRow("evt_test")
			row[test.field] = test.value
			result := validateRows([]map[string]string{row}, 2)
			if len(result.Accepted) != 0 || len(result.Rejected) != 1 {
				t.Fatalf("accepted/rejected = %d/%d, want 0/1", len(result.Accepted), len(result.Rejected))
			}
			if !hasIssue(result.Rejected[0], test.wantedCode) {
				t.Fatalf("issues = %v, want code %s", issueCodes(result.Rejected[0]), test.wantedCode)
			}
		})
	}
}

func TestExactDecimalFingerprinting(t *testing.T) {
	first := validRow("evt_first")
	first["amount"] = "0.10"
	second := validRow("evt_second")
	second["amount"] = "0.100"
	second["ingested_at"] = "10:00:02"

	result := validateRows([]map[string]string{first, second}, 2)
	if len(result.Accepted) != 1 || len(result.Rejected) != 1 {
		t.Fatalf("accepted/rejected = %d/%d, want 1/1", len(result.Accepted), len(result.Rejected))
	}
	assertOnlyIssue(t, result.Rejected[0], DuplicateTrade)
}

func TestLargeDecimalDoesNotOverflowOrLosePrecision(t *testing.T) {
	row := validRow("evt_large")
	row["amount"] = "123456789012345678901234567890.12345678901234567890"
	result := validateRows([]map[string]string{row}, 2)
	if len(result.Accepted) != 1 || len(result.Rejected) != 0 {
		t.Fatalf("accepted/rejected = %d/%d, want 1/0", len(result.Accepted), len(result.Rejected))
	}
	want, _ := new(big.Rat).SetString(row["amount"])
	if result.Accepted[0].Amount.Cmp(want) != 0 {
		t.Fatalf("amount = %s, want %s", result.Accepted[0].Amount.RatString(), want.RatString())
	}
}

func TestSupportedPositiveDecimalFormats(t *testing.T) {
	tests := map[string]string{
		"integer":              "1",
		"fractional":           ".125",
		"trailing decimal dot": "1.",
		"scientific notation":  "1.25e3",
		"explicit plus":        "+5",
	}
	for name, value := range tests {
		t.Run(name, func(t *testing.T) {
			amount, err := parsePositiveDecimal(value)
			if err != nil {
				t.Fatalf("parsePositiveDecimal(%q) error = %v", value, err)
			}
			if amount.Sign() <= 0 {
				t.Fatalf("parsePositiveDecimal(%q) = %s, want positive", value, amount.RatString())
			}
		})
	}
}

func TestTransactionHashAloneIsNotADeDuplicationKey(t *testing.T) {
	first := validRow("evt_first")
	second := validRow("evt_second")
	second["amount"] = "2"
	second["ingested_at"] = "10:00:02"

	result := validateRows([]map[string]string{first, second}, 2)
	if len(result.Accepted) != 2 || len(result.Rejected) != 0 {
		t.Fatalf("accepted/rejected = %d/%d, want 2/0", len(result.Accepted), len(result.Rejected))
	}
}

func TestFingerprintNormalisesIdentifierAndSideCase(t *testing.T) {
	first := validRow("evt_first")
	first["tx_hash"] = "0xAbC"
	first["wallet"] = "0xDeF"
	second := validRow("evt_second")
	second["tx_hash"] = "0xaBc"
	second["wallet"] = "0xdEf"
	second["side"] = "buy"
	second["ingested_at"] = "10:00:02"

	result := validateRows([]map[string]string{first, second}, 2)
	if len(result.Accepted) != 1 || len(result.Rejected) != 1 {
		t.Fatalf("accepted/rejected = %d/%d, want 1/1", len(result.Accepted), len(result.Rejected))
	}
	assertOnlyIssue(t, result.Rejected[0], DuplicateTrade)
}

func TestInvalidFirstOccurrenceDoesNotSuppressValidReplay(t *testing.T) {
	invalid := validRow("evt_invalid")
	invalid["ingested_at"] = "09:59:59"
	valid := validRow("evt_valid")
	valid["ingested_at"] = "10:00:01"

	result := validateRows([]map[string]string{invalid, valid}, 2)
	if len(result.Accepted) != 1 || result.Accepted[0].EventID != "evt_valid" {
		t.Fatalf("accepted events = %v, want only evt_valid", eventIDs(result.Accepted))
	}
	assertOnlyIssue(t, result.Rejected[0], IngestionBeforeBlockTime)
}

func TestDuplicateEventIDIsRejectedEvenWhenTradeDiffers(t *testing.T) {
	first := validRow("evt_same")
	second := validRow("evt_same")
	second["tx_hash"] = "0x2"
	second["amount"] = "2"
	second["block_time"] = "10:01:00"
	second["ingested_at"] = "10:01:01"

	result := validateRows([]map[string]string{first, second}, 2)
	if len(result.Accepted) != 1 || len(result.Rejected) != 1 {
		t.Fatalf("accepted/rejected = %d/%d, want 1/1", len(result.Accepted), len(result.Rejected))
	}
	assertOnlyIssue(t, result.Rejected[0], DuplicateEventID)
}

func TestAcceptedOutputIsSortedByBlockTimeNotArrivalOrder(t *testing.T) {
	later := validRow("evt_later")
	later["tx_hash"] = "0x2"
	later["block_time"] = "11:00:00"
	later["ingested_at"] = "11:00:01"
	earlier := validRow("evt_earlier")
	earlier["tx_hash"] = "0x1"
	earlier["block_time"] = "09:00:00"
	earlier["ingested_at"] = "09:00:01"

	result := validateRows([]map[string]string{later, earlier}, 2)
	want := []string{"evt_earlier", "evt_later"}
	if got := eventIDs(result.Accepted); !reflect.DeepEqual(got, want) {
		t.Fatalf("accepted IDs = %v, want %v", got, want)
	}
}

func TestTimestampFormats(t *testing.T) {
	valid := []string{
		"10:00:00",
		"10:00:00.123456789",
		"10:00:00Z",
		"2026-09-28T10:00:00Z",
		"2026-09-28T10:00:00.123456789+00:00",
	}
	for _, value := range valid {
		t.Run("valid_"+strings.ReplaceAll(value, ":", "_"), func(t *testing.T) {
			parsed, err := parseUTCTimestamp(value)
			if err != nil {
				t.Fatalf("parseUTCTimestamp(%q) error = %v", value, err)
			}
			if parsed.Location() != time.UTC {
				t.Fatalf("location = %v, want UTC", parsed.Location())
			}
		})
	}

	invalid := []string{
		"",
		"10:00",
		"25:00:00",
		"2026-09-28T10:00:00",
		"2026-09-28T10:00:00+03:00",
	}
	for _, value := range invalid {
		t.Run("invalid_"+strings.ReplaceAll(value, ":", "_"), func(t *testing.T) {
			if _, err := parseUTCTimestamp(value); err == nil {
				t.Fatalf("parseUTCTimestamp(%q) unexpectedly succeeded", value)
			}
		})
	}
}

func TestCSVSchemaAndSyntaxFailures(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{name: "empty feed", content: "", want: "CSV is empty"},
		{name: "missing columns", content: "event_id,tx_hash\nevt_1,0x1\n", want: "missing required CSV column"},
		{name: "duplicate column", content: "event_id,event_id\na,b\n", want: "duplicate CSV column"},
		{name: "empty column", content: "event_id,\na,b\n", want: "header column 2 is empty"},
		{name: "invalid CSV", content: "event_id,tx_hash,block_time,wallet,side,amount,ingested_at\n\"unterminated", want: "read CSV record"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := validateCSV(strings.NewReader(test.content))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestCSVSupportsBOMReorderedColumnsAndExtraColumns(t *testing.T) {
	feed := "\uFEFFamount,side,wallet,block_time,tx_hash,event_id,ingested_at,venue\n" +
		"1,BUY,0xwallet,10:00:00,0x1,evt_1,10:00:01,exchange\n"
	result, err := validateCSV(strings.NewReader(feed))
	if err != nil {
		t.Fatalf("validateCSV() error = %v", err)
	}
	if len(result.Accepted) != 1 || len(result.Rejected) != 0 {
		t.Fatalf("accepted/rejected = %d/%d, want 1/0", len(result.Accepted), len(result.Rejected))
	}
}

func TestCSVReportsPhysicalSourceLineAfterBlankLine(t *testing.T) {
	header := "event_id,tx_hash,block_time,wallet,side,amount,ingested_at\n"
	feed := header + "\n" + "evt_1,0x1,,0xwallet,BUY,1,10:00:01\n"
	result, err := validateCSV(strings.NewReader(feed))
	if err != nil {
		t.Fatalf("validateCSV() error = %v", err)
	}
	if len(result.Rejected) != 1 {
		t.Fatalf("rejected rows = %d, want 1", len(result.Rejected))
	}
	if result.Rejected[0].SourceRow != 3 {
		t.Fatalf("source row = %d, want physical line 3", result.Rejected[0].SourceRow)
	}
}

func TestMalformedCSVRowsAreQuarantined(t *testing.T) {
	header := "event_id,tx_hash,block_time,wallet,side,amount,ingested_at\n"
	feed := header + "evt_1,0x1,10:00:00,0xwallet,BUY,1,10:00:01,unexpected\n"
	result, err := validateCSV(strings.NewReader(feed))
	if err != nil {
		t.Fatalf("validateCSV() error = %v", err)
	}
	if len(result.Accepted) != 0 || len(result.Rejected) != 1 {
		t.Fatalf("accepted/rejected = %d/%d, want 0/1", len(result.Accepted), len(result.Rejected))
	}
	assertOnlyIssue(t, result.Rejected[0], MalformedRow)
}

func TestShortCSVRowIsQuarantinedWithAllReasons(t *testing.T) {
	header := "event_id,tx_hash,block_time,wallet,side,amount,ingested_at\n"
	feed := header + "evt_1,0x1,10:00:00,0xwallet,BUY,1\n"
	result, err := validateCSV(strings.NewReader(feed))
	if err != nil {
		t.Fatalf("validateCSV() error = %v", err)
	}
	if len(result.Rejected) != 1 {
		t.Fatalf("rejected rows = %d, want 1", len(result.Rejected))
	}
	if !hasIssue(result.Rejected[0], MalformedRow) ||
		!hasIssue(result.Rejected[0], MissingRequiredField) {
		t.Fatalf("issues = %v, want malformed and missing-field codes", issueCodes(result.Rejected[0]))
	}
}

func TestCLIExitCodes(t *testing.T) {
	t.Run("sample succeeds and reports rejections", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		exitCode := runCLI(
			[]string{"-input", "sample_feed.csv", "-output-dir", t.TempDir()},
			&stdout,
			&stderr,
		)
		if exitCode != 0 {
			t.Fatalf("exit code = %d, want 0; stderr = %s", exitCode, stderr.String())
		}
		if !strings.Contains(stdout.String(), "Accepted rows: 4") ||
			!strings.Contains(stdout.String(), "Rejected rows: 4") {
			t.Fatalf("stdout = %q, want accepted/rejected summary", stdout.String())
		}
	})

	t.Run("strict mode fails on rejected rows", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		exitCode := runCLI(
			[]string{"-input", "sample_feed.csv", "-output-dir", t.TempDir(), "-fail-on-rejections"},
			&stdout,
			&stderr,
		)
		if exitCode != 1 {
			t.Fatalf("exit code = %d, want 1; stderr = %s", exitCode, stderr.String())
		}
	})

	t.Run("configuration error", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		exitCode := runCLI([]string{"unexpected"}, &stdout, &stderr)
		if exitCode != 2 {
			t.Fatalf("exit code = %d, want 2", exitCode)
		}
	})

	t.Run("unknown flag", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		exitCode := runCLI([]string{"-unknown"}, &stdout, &stderr)
		if exitCode != 2 {
			t.Fatalf("exit code = %d, want 2", exitCode)
		}
	})

	t.Run("pipeline error", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		exitCode := runCLI(
			[]string{"-input", filepath.Join(t.TempDir(), "missing.csv")},
			&stdout,
			&stderr,
		)
		if exitCode != 2 || !strings.Contains(stderr.String(), "open input feed") {
			t.Fatalf("exit code/stderr = %d/%q, want 2 and input error", exitCode, stderr.String())
		}
	})

	t.Run("stdout error", func(t *testing.T) {
		var stderr bytes.Buffer
		exitCode := runCLI(
			[]string{"-input", "sample_feed.csv", "-output-dir", t.TempDir()},
			failingWriter{},
			&stderr,
		)
		if exitCode != 2 || !strings.Contains(stderr.String(), "write command output") {
			t.Fatalf("exit code/stderr = %d/%q, want 2 and output error", exitCode, stderr.String())
		}
	})
}

func loadSampleFeed(t *testing.T) ValidationResult {
	t.Helper()
	file, err := os.Open("sample_feed.csv")
	if err != nil {
		t.Fatalf("open sample feed: %v", err)
	}
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Errorf("close sample feed: %v", err)
		}
	})
	result, err := validateCSV(file)
	if err != nil {
		t.Fatalf("validate sample feed: %v", err)
	}
	return result
}

func rejectedEvent(t *testing.T, result ValidationResult, eventID string) RejectedEvent {
	t.Helper()
	for _, event := range result.Rejected {
		if event.EventID == eventID {
			return event
		}
	}
	t.Fatalf("rejected event %q not found", eventID)
	return RejectedEvent{}
}

func assertOnlyIssue(t *testing.T, event RejectedEvent, wanted IssueCode) {
	t.Helper()
	if len(event.Issues) != 1 || event.Issues[0].Code != wanted {
		t.Fatalf("issues = %v, want only %s", issueCodes(event), wanted)
	}
}

func hasIssue(event RejectedEvent, wanted IssueCode) bool {
	for _, issue := range event.Issues {
		if issue.Code == wanted {
			return true
		}
	}
	return false
}

func issueCodes(event RejectedEvent) []IssueCode {
	codes := make([]IssueCode, 0, len(event.Issues))
	for _, issue := range event.Issues {
		codes = append(codes, issue.Code)
	}
	return codes
}

func eventIDs(events []TradeEvent) []string {
	ids := make([]string, 0, len(events))
	for _, event := range events {
		ids = append(ids, event.EventID)
	}
	return ids
}

func validRow(eventID string) map[string]string {
	return map[string]string{
		"event_id":    eventID,
		"tx_hash":     "0x1",
		"block_time":  "10:00:00",
		"wallet":      "0xwallet",
		"side":        "BUY",
		"amount":      "1",
		"ingested_at": "10:00:01",
	}
}

func readCSVFile(t *testing.T, path string) [][]string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Errorf("close %s: %v", path, err)
		}
	})
	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return records
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("injected writer failure")
}
