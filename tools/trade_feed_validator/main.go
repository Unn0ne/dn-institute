package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

func main() {
	os.Exit(runCLI(os.Args[1:], os.Stdout, os.Stderr))
}

func runCLI(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("trade-feed-validator", flag.ContinueOnError)
	flags.SetOutput(stderr)
	inputPath := flags.String("input", "sample_feed.csv", "path to the input CSV feed")
	outputDir := flags.String("output-dir", "output", "directory for validation outputs")
	failOnRejections := flags.Bool(
		"fail-on-rejections",
		false,
		"exit with status 1 if any row is rejected",
	)
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		_, _ = io.WriteString(stderr, fmt.Sprintf("unexpected positional arguments: %v\n", flags.Args()))
		return 2
	}

	result, err := runPipeline(*inputPath, *outputDir)
	if err != nil {
		_, _ = io.WriteString(stderr, fmt.Sprintf("error: %v\n", err))
		return 2
	}

	lines := []string{
		fmt.Sprintf("Input rows: %d", result.InputRows),
		fmt.Sprintf("Accepted rows: %d", len(result.Accepted)),
		fmt.Sprintf("Rejected rows: %d", len(result.Rejected)),
		"Issues:",
	}
	counts := result.IssueCounts()
	for _, code := range sortedIssueCodes(counts) {
		lines = append(lines, fmt.Sprintf("  %s: %d", code, counts[code]))
	}
	lines = append(lines, fmt.Sprintf("Outputs: %s", result.RunDir))
	if _, err := io.WriteString(stdout, strings.Join(lines, "\n")+"\n"); err != nil {
		_, _ = io.WriteString(stderr, fmt.Sprintf("error: write command output: %v\n", err))
		return 2
	}

	if *failOnRejections && len(result.Rejected) > 0 {
		return 1
	}
	return 0
}
