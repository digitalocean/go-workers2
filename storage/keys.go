package storage

import "fmt"

var heartbeatWorkKey = ":work"

// GetManagerKey gets redis key for manager
func GetManagerKey(namespace, heartbeatID string) string {
	return namespace + heartbeatID
}

// GetWorkersKey gets redis key for manager's workers' heartbeat
func GetWorkersKey(managerKey string) string {
	return managerKey + heartbeatWorkKey
}

// GetWorkerID gets a worker's ID
func GetWorkerID(pid int, tid string) string {
	return fmt.Sprintf("%d-%s", pid, tid)
}

// GetProcessesKey gets redis key for manager processes
func GetProcessesKey(namespace string) string {
	return namespace + "processes"
}
