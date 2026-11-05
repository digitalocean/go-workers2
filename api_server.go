package workers

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
)

// APIOptions contains the set of configuration options for the global api
type APIOptions struct {
	Logger *slog.Logger
	Mux    *http.ServeMux
}

type apiServer struct {
	lock     sync.Mutex
	managers map[string]*Manager
	logger   *slog.Logger
	mux      *http.ServeMux
}

func (s *apiServer) registerManager(m *Manager) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.managers == nil {
		s.managers = make(map[string]*Manager)
	}
	s.managers[m.uuid] = m
}

func (s *apiServer) deregisterManager(m *Manager) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.managers, m.uuid)
}

func (s *apiServer) registeredManagers() []*Manager {
	s.lock.Lock()
	defer s.lock.Unlock()
	managers := make([]*Manager, 0, len(s.managers))
	for _, m := range s.managers {
		managers = append(managers, m)
	}
	return managers
}

var globalHTTPServer *http.Server

var globalAPIServer = &apiServer{
	managers: map[string]*Manager{},
	logger:   slog.Default(),
	mux:      http.NewServeMux(),
}

// ConfigureAPIServer allows global API server configuration with the given options
func ConfigureAPIServer(options APIOptions) {
	if options.Logger != nil {
		globalAPIServer.logger = options.Logger
	}

	if options.Mux != nil {
		globalAPIServer.mux = options.Mux
	}
}

// RegisterAPIEndpoints sets up API server endpoints
func RegisterAPIEndpoints(mux *http.ServeMux) {
	mux.HandleFunc("/stats", globalAPIServer.Stats)
	mux.HandleFunc("/retries", globalAPIServer.Retries)
	mux.HandleFunc("/dead", globalAPIServer.Dead)
}

// StartAPIServer starts the API server
func StartAPIServer(port int) {
	RegisterAPIEndpoints(globalAPIServer.mux)

	globalAPIServer.logger.Info("APIs are available", "url", fmt.Sprintf("http://localhost:%v/", port))

	globalHTTPServer = &http.Server{Addr: fmt.Sprint(":", port), Handler: globalAPIServer.mux}
	if err := globalHTTPServer.ListenAndServe(); err != nil {
		globalAPIServer.logger.Error("API server failed", "error", err)
	}
}

// StopAPIServer stops the API server
func StopAPIServer() {
	if globalHTTPServer != nil {
		globalHTTPServer.Shutdown(context.Background())
	}
}
