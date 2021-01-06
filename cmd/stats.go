package cmd

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/spf13/cobra"
)

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "go-workers2 stats info",
	Long: `Use the stats command to get stats from a specified host address and
	port number, like so:

	goworkersctl stats --a 127.0.0.1 --p 8080`,
	RunE: runStats,
}

func init() {
	rootCmd.AddCommand(statsCmd)
}

func runStats(cmd *cobra.Command, args []string) error {
	address := "http://" + hostAddress + ":" + port + "/stats"

	resp, err := http.Get(address)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	fmt.Printf("Body: %v\n", string(body))
	return nil
}
