package workers

import (
	"os/signal"
	"syscall"
)

func (m *Manager) handleSignals() {
	signal.Notify(m.signal, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(m.signal)

	for sig := range m.signal {
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
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
