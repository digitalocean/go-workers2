package workers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func buildFetch(queue string, opts Options) Fetcher {
	fetch := newSimpleFetcher(queue, opts)
	go fetch.Fetch()
	return fetch
}

func TestFetchConfig(t *testing.T) {
	opts, err := setupTestOptions()
	assert.NoError(t, err)
	fetch := buildFetch("fetchQueue1", opts)
	assert.Equal(t, "queue:fetchQueue1", fetch.Queue())
	fetch.Close()
}

func TestGetMessagesToChannel(t *testing.T) {
	opts, err := setupTestOptions()
	assert.NoError(t, err)

	message, _ := NewMsg("{\"foo\":\"bar\"}")
	fetch := buildFetch("fetchQueue2", opts)

	rc := opts.client

	rc.LPush("queue:fetchQueue2", message.ToJson()).Result()

	fetch.Ready() <- true
	fetchedMessage := <-fetch.Messages()

	assert.Equal(t, message, fetchedMessage)

	len, err := rc.LLen("queue:fetchQueue2").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), len)

	fetch.Close()
}

func TestMoveProgressMessageToPrivateQueue(t *testing.T) {
	opts, err := setupTestOptions()
	assert.NoError(t, err)
	message, _ := NewMsg("{\"foo\":\"bar\"}")

	fetch := buildFetch("fetchQueue3", opts)

	rc := opts.client

	rc.LPush("queue:fetchQueue3", message.ToJson())

	fetch.Ready() <- true
	<-fetch.Messages()

	len, err := rc.LLen("queue:fetchQueue3:1:inprogress").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), len)

	messages, err := rc.LRange("queue:fetchQueue3:1:inprogress", 0, -1).Result()
	assert.NoError(t, err)
	assert.Equal(t, message.ToJson(), messages[0])

	fetch.Close()
}

func TestRemoveProgressMessageWhenAcked(t *testing.T) {
	opts, err := setupTestOptions()
	assert.NoError(t, err)
	message, _ := NewMsg("{\"foo\":\"bar\"}")

	fetch := buildFetch("fetchQueue4", opts)

	rc := opts.client

	rc.LPush("queue:fetchQueue4", message.ToJson()).Result()

	fetch.Ready() <- true
	<-fetch.Messages()

	fetch.Acknowledge(message)

	len, err := rc.LLen("queue:fetchQueue4:1:inprogress").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), len)

	fetch.Close()
}

func TestRemoveProgressMessageDifferentSerialization(t *testing.T) {
	opts, err := setupTestOptions()
	assert.NoError(t, err)

	json := "{\"foo\":\"bar\",\"args\":[]}"
	message, _ := NewMsg(json)

	assert.NotEqual(t, message.ToJson(), json)

	fetch := buildFetch("fetchQueue5", opts)

	rc := opts.client

	rc.LPush("queue:fetchQueue5", json).Result()

	fetch.Ready() <- true
	<-fetch.Messages()

	fetch.Acknowledge(message)

	len, err := rc.LLen("queue:fetchQueue5:1:inprogress").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), len)

	fetch.Close()
}

func TestRetryInprogressMessages(t *testing.T) {
	opts, err := setupTestOptions()
	assert.NoError(t, err)

	message, _ := NewMsg("{\"foo\":\"bar\"}")
	message2, _ := NewMsg("{\"foo\":\"bar2\"}")
	message3, _ := NewMsg("{\"foo\":\"bar3\"}")

	rc := opts.client

	rc.LPush("queue:fetchQueue6:1:inprogress", message.ToJson()).Result()
	rc.LPush("queue:fetchQueue6:1:inprogress", message2.ToJson()).Result()
	rc.LPush("queue:fetchQueue6", message3.ToJson()).Result()

	fetch := buildFetch("fetchQueue6", opts)

	fetch.Ready() <- true
	assert.Equal(t, message2, <-fetch.Messages())
	fetch.Ready() <- true
	assert.Equal(t, message, <-fetch.Messages())
	fetch.Ready() <- true
	assert.Equal(t, message3, <-fetch.Messages())

	fetch.Acknowledge(message)
	fetch.Acknowledge(message2)
	fetch.Acknowledge(message3)

	len, err := rc.LLen("queue:fetchQueue6:1:inprogress").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), len)

	fetch.Close()
}
