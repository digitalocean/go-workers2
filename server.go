package workers

import (
	"context"
	"fmt"
	"net/http"
)

var globalHTTPServer *http.Server

func StartAPIServer(port int) {
	mux := http.NewServeMux()

	mux.HandleFunc("/stats", globalApiServer.Stats)
	mux.HandleFunc("/retries", globalApiServer.Retries)

	Logger.Println("APIs are available at", fmt.Sprintf("http://localhost:%v/", port))

	globalHTTPServer = &http.Server{Addr: fmt.Sprint(":", port), Handler: mux}
	if err := globalHTTPServer.ListenAndServe(); err != nil {
		Logger.Println(err)
	}
}

func StopAPIServer() {
	if globalHTTPServer != nil {
		globalHTTPServer.Shutdown(context.Background())
	}
}
