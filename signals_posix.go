// +build !windows

package workers

import (
	"os/signal"
	"syscall"
)

func (m *Manager) handleSignals() {
	signal.Notify(m.signal, syscall.SIGUSR1, syscall.SIGINT, syscall.SIGTERM)

	for sig := range m.signal {
		switch sig {
		case syscall.SIGINT, syscall.SIGUSR1, syscall.SIGTERM:
			m.Stop()
			// Don't stop more than once.
			return
		}
	}
}

func (m *Manager) stopSignalHandler() {
	signal.Stop(m.signal)
	close(m.signal)
}
