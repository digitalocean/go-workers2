package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	hostAddress string
	port        string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "gwctl",
	Short: "gwctl is the GoWorkers2 cli tool",
	Long: `
                                               __                       ________
    ____   ____           __  _  _____________|  | __ ___________  _____\_____  \
   / ___\ /  _ \   ______ \ \/ \/ /  _ \_  __ \  |/ // __ \_  __ \/  ___//  ____/
  / /_/  >  <_> ) /_____/  \     (  <_> )  | \/    <\  ___/|  | \/\___ \/       \
  \___  / \____/            \/\_/ \____/|__|  |__|_ \\____ >__|  /_____ >________\
 /_____/

  gwctl is a cli tool that allows a user to easily get data from a go-workers2 instance.`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&hostAddress, "a", "localhost", "Host address for a specific goworkers2 instance.")
	rootCmd.PersistentFlags().StringVar(&port, "p", "8080", "Port number for a specific goworkers2 instance.")
}
