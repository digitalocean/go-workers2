/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// queueCmd represents the queue command
var queueCmd = &cobra.Command{
	Use:     "queue",
	Aliases: []string{"q"},
	Short:   "The queue represents the worker queue",
	Long: `A queue contains jobs. These jobs can be currently processing jobs, retried jobs or failed jobs. An example command could be:

gw -q retry

which would return something like this:

[
	{
	"total_retry_count": 14,
	"retry_jobs": [
		{
			"class": "Add",
			"error_message": "Throw an error for testing retries",
			"failed_at": "2019-12-13 14:01:12 UTC",
			"jid": "36d8f48d8bf6e8fe4ee31437",
			"queue": "myqueue3",
			"retry_count": 14
		},
	}
]`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("queue called")
	},
}

func init() {
	rootCmd.AddCommand(queueCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// queueCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// queueCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
