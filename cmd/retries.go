package cmd

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/spf13/cobra"
)

var retriesCmd = &cobra.Command{
	Use:   "retries",
	Short: "go-workers2 retries info",
	Long: `Use the retries command to get retries information from a specified host address and
	port number, like so:

	goworkersctl retries --a 127.0.0.1 --p 8080`,
	RunE: runRetries,
}

func init() {
	rootCmd.AddCommand(retriesCmd)
}

func runRetries(cmd *cobra.Command, args []string) error {
	address := "http://" + hostAddress + ":" + port + "/retries"

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
