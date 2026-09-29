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
)

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

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return ValidationResult{}, fmt.Errorf("create output directory: %w", err)
	}

	outputs := []struct {
		name  string
		write func(io.Writer) error
	}{
		{validOutputName, func(writer io.Writer) error { return writeValidEvents(writer, result) }},
		{rejectedOutputName, func(writer io.Writer) error { return writeRejectedEvents(writer, result) }},
		{reportOutputName, func(writer io.Writer) error { return writeReport(writer, result) }},
	}
	for _, output := range outputs {
		path := filepath.Join(outputDir, output.name)
		if err := writeAtomically(path, output.write); err != nil {
			return ValidationResult{}, fmt.Errorf("write %s: %w", output.name, err)
		}
	}

	return result, nil
}

func writeValidEvents(destination io.Writer, result ValidationResult) error {
	writer := csv.NewWriter(destination)
	if err := writer.Write(csvFields); err != nil {
		return err
	}
	for _, event := range result.Accepted {
		if err := writer.Write(recordFromRow(event.Raw, csvFields)); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeRejectedEvents(destination io.Writer, result ValidationResult) error {
	fields := append([]string(nil), csvFields...)
	fields = append(fields, "source_row", "validation_codes", "validation_messages")
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

		record := recordFromRow(event.Raw, csvFields)
		record = append(
			record,
			fmt.Sprintf("%d", event.SourceRow),
			strings.Join(codes, "|"),
			strings.Join(messages, "|"),
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
