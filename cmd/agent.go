package main

import (
	"git.querycap.com/falcontsdb/fctsdb-bench/agent"
	"github.com/spf13/cobra"
)

var (
	agentService = &agent.AgentService{}

	agentCmd = &cobra.Command{
		Use:   "agent",
		Short: "代理程序，和数据库运行在一起，支持被远程调用开启关闭数据库（开发团队内部使用）",
		Run: func(cmd *cobra.Command, args []string) {
			agentService.ListenAndServe()
			// GetPidOnLinux("fctsdb")
		},
		Hidden: !FullFunction, // 隐藏此命令，不对外使用，内部测试使用
	}
)

func init() {
	BindCmd(agentService, agentCmd)
}

func BindCmd(s *agent.AgentService, cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.StringVar(&s.Port, "port", "8966", "监听端口")
	flags.StringVar(&s.BinPath, "fctsdb-path", "./fctsdb", "数据库二进制文件地址")
	flags.StringVar(&s.ConfigPath, "fctsdb-config", "./config", "数据库config文件地址")
	flags.StringVar(&s.Format, "format", "fctsdb", "数据库的类型，当前仅支持fctsdb和mysql，默认为fctsdb")
}
