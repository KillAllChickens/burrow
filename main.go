package main

import (
	"fmt"
	"os"

	"github.com/pion/webrtc/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"burrow/config"
	"burrow/conn"
	"burrow/receive"
	"burrow/send"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "burrow",
		Short: "Send and recieve files, encrypted and secure",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			config.InitConfig()
		},

		// Run: func(cmd *cobra.Command, args []string) {
		// 	cobra.help

		// },
	}

	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start a server",
		Run: func(cmd *cobra.Command, args []string) {
			filePath, _ := cmd.Flags().GetString("file")
			if filePath == "" {
				fmt.Println("[!] Error: You must specify a file to share using -f or --file")
				os.Exit(1)
			}

			onChannelOpen := func(dc *webrtc.DataChannel) {
				send.HandleFileSend(dc, filePath)
			}

			fmt.Println("[*] Initializing file sharing server...")

			conn.Initialize(true, "", onChannelOpen)
		},
	}

	joinCmd := &cobra.Command{
		Use:   "join",
		Short: "join a server",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			code := args[0]
			outputDir, _ := cmd.Flags().GetString("output")
			if outputDir == "" {
				outputDir = "."
			}

			onChannelOpen := func(dc *webrtc.DataChannel) {
				receive.HandleFileReceive(dc, outputDir)
			}

			conn.Initialize(false, code, onChannelOpen)
		},
	}

	startCmd.Flags().StringP("file", "f", "", "The file to share (Required)")
	joinCmd.Flags().StringP("output", "o", "", "The directory directory to save the file in (defaults to current dir)")

	viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	viper.BindPFlag("server", rootCmd.PersistentFlags().Lookup("server"))

	rootCmd.AddCommand(startCmd)
	rootCmd.AddCommand(joinCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
