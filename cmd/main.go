package main

import (
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/spf13/cobra"
)

const TOOL_NAME = "fcbench"

var (
	BuildVersion string
	FullFunction bool = true
	rootCmd           = &cobra.Command{
		Use:   TOOL_NAME + " [command] [flags]",
		Short: TOOL_NAME + " 一个用于海东青数据库的测试工具",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Help()
		},
		Version:           BuildVersion,
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
		// helpCommand:       helpCmd,
	}

	mockCmd = &cobra.Command{
		Use:   "mock",
		Short: "模仿海东青数据库，测试本工具能力上限",
		Run: func(cmd *cobra.Command, args []string) {
			mockFctsdb()
		}, // Hidden: true,
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
		// io.WriteString(w, "ok")
		io.WriteString(w, `{"results":[{"statement_id":0,"series":[{"name":"city_air_quality","tags":{"site_id":"DEV000008449"},"columns":["time","aqi"],"values":[["2018-01-15T23:59:00Z",222]]}]}]}`)
	})
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		// log.Println("ping")
		w.WriteHeader(204)
		io.WriteString(w, "")
	})
	http.HandleFunc("/clean", func(w http.ResponseWriter, r *http.Request) {
		log.Println("clean db data")
		w.WriteHeader(200)
		io.WriteString(w, "")
	})
	http.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		log.Println("start db")
		w.WriteHeader(200)
		io.WriteString(w, "")
	})
	http.HandleFunc("/stop", func(w http.ResponseWriter, r *http.Request) {
		log.Println("stop db")
		w.WriteHeader(200)
		io.WriteString(w, "")
	})
	log.Println("Start service 0.0.0.0:9086")
	// server.ListenAndServe()
	log.Println(http.ListenAndServe("0.0.0.0:9086", nil))
}

func main() {
	cobra.EnableCommandSorting = false
	fmt.Println("多源异构数据融合分析与处理技术V2.0")
	rootCmd.Flags().BoolP("version", "v", false, "查看版本信息")
	rootCmd.PersistentFlags().BoolP("help", "h", false, "查看帮助信息")

	rootCmd.AddCommand(listQueryCmd)
	rootCmd.AddCommand(writeCmd)
	rootCmd.AddCommand(queryCmd)
	rootCmd.AddCommand(mixedCmd)
	rootCmd.AddCommand(mockCmd)
	rootCmd.AddCommand(agentCmd)
	rootCmd.AddCommand(scheduleCmd)
	// 隐藏命令
	rootCmd.AddCommand(dataGenCmd)
	rootCmd.AddCommand(dataLoadCmd)
	rootCmd.AddCommand(queryGenCmd)
	rootCmd.AddCommand(queryLoadCmd)

	rootCmd.SetHelpCommand(&cobra.Command{})
	rootCmd.Execute()

}
