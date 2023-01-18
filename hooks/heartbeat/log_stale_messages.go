package heartbeat

import (
	"fmt"

	workers "github.com/digitalocean/go-workers2"
	"github.com/digitalocean/go-workers2/storage"
)

func LogStaleMessagesAfterHeartbeatHook(heartbeat *storage.Heartbeat, manager *workers.Manager, staleMessageUpdates []*storage.StaleMessageUpdate) error {
	if len(staleMessageUpdates) > 0 {
		for _, staleMessageUpdate := range staleMessageUpdates {
			for _, msg := range staleMessageUpdate.RequeuedMsgs {
				manager.Opts().Logger.Println(fmt.Sprintf("[%s] Requeued message: {'inProgressQueue': '%s', 'queue': %s', 'message': '%s'}",
					heartbeat.Beat.String(), staleMessageUpdate.InprogressQueue, staleMessageUpdate.Queue, msg))
			}
		}
	}
	return nil
}
