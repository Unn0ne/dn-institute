package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	validOutputName    = "valid_trades.csv"
	rejectedOutputName = "rejected_events.csv"
	reportOutputName   = "validation_report.json"
	latestOutputName   = "LATEST"
)

type outputFile struct {
	name  string
	write func(io.Writer) error
}

type validationSummary struct {
	InputRows    int               `json:"input_rows"`
	AcceptedRows int               `json:"accepted_rows"`
	RejectedRows int               `json:"rejected_rows"`
	IssueCounts  map[IssueCode]int `json:"issue_counts"`
}

type validationReport struct {
	Summary validationSummary `json:"summary"`
	Issues  []ValidationIssue `json:"issues"`
}

func runPipeline(inputPath, outputDir string) (ValidationResult, error) {
	input, err := os.Open(inputPath)
	if err != nil {
		return ValidationResult{}, fmt.Errorf("open input feed: %w", err)
	}

	result, err := validateCSV(input)
	closeErr := input.Close()
	if err != nil {
		return ValidationResult{}, err
	}
	if closeErr != nil {
		return ValidationResult{}, fmt.Errorf("close input feed: %w", closeErr)
	}

	outputs := []outputFile{
		{validOutputName, func(writer io.Writer) error { return writeValidEvents(writer, result) }},
		{rejectedOutputName, func(writer io.Writer) error { return writeRejectedEvents(writer, result) }},
		{reportOutputName, func(writer io.Writer) error { return writeReport(writer, result) }},
	}
	runDir, err := publishOutputs(outputDir, outputs)
	if err != nil {
		return ValidationResult{}, err
	}
	result.RunDir = runDir
	return result, nil
}

func publishOutputs(outputDir string, outputs []outputFile) (string, error) {
	runsDir := filepath.Join(outputDir, "runs")
	if err := os.MkdirAll(runsDir, 0o755); err != nil {
		return "", fmt.Errorf("create output directory: %w", err)
	}

	runDir, err := os.MkdirTemp(runsDir, "run-")
	if err != nil {
		return "", fmt.Errorf("create output run: %w", err)
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(runDir)
		}
	}()

	for _, output := range outputs {
		path := filepath.Join(runDir, output.name)
		if err := writeAtomically(path, output.write); err != nil {
			return "", fmt.Errorf("write %s: %w", output.name, err)
		}
	}

	latestPath := filepath.Join(outputDir, latestOutputName)
	runPath := filepath.Join("runs", filepath.Base(runDir))
	if err := writeAtomically(latestPath, func(writer io.Writer) error {
		_, err := io.WriteString(writer, runPath+"\n")
		return err
	}); err != nil {
		return "", fmt.Errorf("publish output run: %w", err)
	}

	published = true
	return runDir, nil
}

func writeValidEvents(destination io.Writer, result ValidationResult) error {
	writer := csv.NewWriter(destination)
	if err := writer.Write(result.Columns); err != nil {
		return err
	}
	for _, event := range result.Accepted {
		if err := writer.Write(recordFromRow(event.Raw, result.Columns)); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeRejectedEvents(destination io.Writer, result ValidationResult) error {
	fields := append([]string(nil), result.Columns...)
	fields = append(fields, "source_row", "validation_codes", "validation_messages", "unmapped_values_json")
	writer := csv.NewWriter(destination)
	if err := writer.Write(fields); err != nil {
		return err
	}

	for _, event := range result.Rejected {
		codes := make([]string, 0, len(event.Issues))
		messages := make([]string, 0, len(event.Issues))
		for _, issue := range event.Issues {
			codes = append(codes, string(issue.Code))
			messages = append(messages, issue.Message)
		}
		unmappedValues := ""
		if len(event.UnmappedValues) > 0 {
			encoded, err := json.Marshal(event.UnmappedValues)
			if err != nil {
				return err
			}
			unmappedValues = string(encoded)
		}

		record := recordFromRow(event.Raw, result.Columns)
		record = append(
			record,
			fmt.Sprintf("%d", event.SourceRow),
			strings.Join(codes, "|"),
			strings.Join(messages, "|"),
			unmappedValues,
		)
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeReport(destination io.Writer, result ValidationResult) error {
	report := validationReport{
		Summary: validationSummary{
			InputRows:    result.InputRows,
			AcceptedRows: len(result.Accepted),
			RejectedRows: len(result.Rejected),
			IssueCounts:  result.IssueCounts(),
		},
		Issues: result.Issues(),
	}

	encoder := json.NewEncoder(destination)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

func writeAtomically(path string, writeData func(io.Writer) error) error {
	dir := filepath.Dir(path)
	tmpFile, err := os.CreateTemp(dir, ".trade-feed-validator-*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()

	if err := writeData(tmpFile); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return nil
}

func recordFromRow(row map[string]string, fields []string) []string {
	record := make([]string, len(fields))
	for index, field := range fields {
		record[index] = row[field]
	}
	return record
}

func sortedIssueCodes(counts map[IssueCode]int) []IssueCode {
	codes := make([]IssueCode, 0, len(counts))
	for code := range counts {
		codes = append(codes, code)
	}
	sort.Slice(codes, func(i, j int) bool { return codes[i] < codes[j] })
	return codes
}
