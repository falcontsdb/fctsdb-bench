package main

import (
	"io"
	"log"
	"net/http"

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
		Version:           "v1.0.1",
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
		// helpCommand:       helpCmd,
	}

	mockCmd = &cobra.Command{
		Use:   "mock",
		Short: "模仿海东青数据库，测试本工具能力上限",
		Run: func(cmd *cobra.Command, args []string) {
			mockFctsdb()
		},
		// Hidden: true,
		// helpCommand:       helpCmd,
	}
)

func mockFctsdb() {

	// server := &http.Server{
	// 	Addr:              "0.0.0.0:9086",
	// 	Handler:           nil,
	// 	ReadTimeout:       time.Second * 10,
	// 	ReadHeaderTimeout: time.Second * 10}

	http.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, 32*1024)
		for {
			_, err := r.Body.Read(b)
			if err == io.EOF {
				break
			}
		}
		w.WriteHeader(204)
		io.WriteString(w, "")
	})
	http.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, 4*1024)
		for {
			_, err := r.Body.Read(b)
			if err == io.EOF {
				break
			}
		}
		w.WriteHeader(200)
		io.WriteString(w, "ok")
	})
	log.Println("Start service 0.0.0.0:9086")
	// server.ListenAndServe()
	log.Println(http.ListenAndServe("0.0.0.0:9086", nil))
}

func main() {

	rootCmd.Flags().BoolP("version", "v", false, "查看版本信息")
	rootCmd.PersistentFlags().BoolP("help", "h", false, "查看帮助信息")
	rootCmd.AddCommand(mockCmd)
	rootCmd.SetHelpCommand(&cobra.Command{})
	rootCmd.Execute()
}
