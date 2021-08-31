package main

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "fcbench",
	Short: "fcbench is a test tool for fctsdb benchmark",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
	Version:           "0.0.1",
	CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
}

func Execute() {
	rootCmd.Execute()
}

func main() {
	Execute()
}
