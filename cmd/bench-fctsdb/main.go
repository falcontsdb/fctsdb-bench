package main

import (
	"github.com/spf13/cobra"
)

const TOOL_NAME = "fcbench"

var (
	rootCmd = &cobra.Command{
		Use:   TOOL_NAME + " [command] [flags]",
		Short: TOOL_NAME + " 一个用于海东青数据库的测试工具",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Help()
		},
		Version:           "0.0.1",
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
		// helpCommand:       helpCmd,
	}
)

func main() {
	rootCmd.Flags().BoolP("version", "v", false, "查看版本信息")
	rootCmd.PersistentFlags().BoolP("help", "h", false, "查看帮助信息")
	rootCmd.SetHelpCommand(&cobra.Command{})
	rootCmd.Execute()
}
