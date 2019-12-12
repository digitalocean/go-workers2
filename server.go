package workers

import (
	"fmt"
	"net/http"
)

func StartAPIServer(port int) {
	mux := http.NewServeMux()

	mux.HandleFunc("/stats", globalApiServer.Stats)
	mux.HandleFunc("/retries", globalApiServer.Retries)

	Logger.Println("APIs are available at", fmt.Sprintf("http://localhost:%v/", port))
	if err := http.ListenAndServe(fmt.Sprint(":", port), mux); err != nil {
		Logger.Println(err)
	}
}
