package storage

import (
	"context"
)

type Store interface {
	SetHeartbeatManagerPriority(ctx context.Context, heartbeatID string, managerPriority int) error
	GetHeartbeatManagerPriorities(ctx context.Context) ([]*HeartbeatManagerPriority, error)

	SetTotalActiveManagers(ctx context.Context, totalActiveManagers int) error
	GetTotalActiveManagers(ctx context.Context) (int, error)
}
