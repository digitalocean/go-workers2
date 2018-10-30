package workers

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProducer_Enqueue(t *testing.T) {
	namespace := "prod"
	opts, err := setupTestOptionsWithNamespace(namespace)
	assert.NoError(t, err)
	rc := opts.client
	p := &Producer{opts: opts}

	//makes the queue available
	p.Enqueue("enqueue1", "Add", []int{1, 2})

	found, _ := rc.SIsMember("prod:queues", "enqueue1").Result()
	assert.True(t, found)

	// adds a job to the queue
	nb, _ := rc.LLen("prod:queue:enqueue2").Result()
	assert.Equal(t, int64(0), nb)

	p.Enqueue("enqueue2", "Add", []int{1, 2})

	nb, _ = rc.LLen("prod:queue:enqueue2").Result()
	assert.Equal(t, int64(1), nb)

	//saves the arguments
	p.Enqueue("enqueue3", "Compare", []string{"foo", "bar"})

	bytes, _ := rc.LPop("prod:queue:enqueue3").Result()
	var result map[string]interface{}
	err = json.Unmarshal([]byte(bytes), &result)
	assert.NoError(t, err)
	assert.Equal(t, "Compare", result["class"])

	args := result["args"].([]interface{})
	assert.Len(t, args, 2)
	assert.Equal(t, "foo", args[0])
	assert.Equal(t, "bar", args[1])

	//has a jid
	p.Enqueue("enqueue4", "Compare", []string{"foo", "bar"})

	bytes, _ = rc.LPop("prod:queue:enqueue4").Result()
	err = json.Unmarshal([]byte(bytes), &result)
	assert.NoError(t, err)
	assert.Equal(t, "Compare", result["class"])

	jid := result["jid"].(string)
	assert.Len(t, jid, 24)

	//has enqueued_at that is close to now
	p.Enqueue("enqueue5", "Compare", []string{"foo", "bar"})

	bytes, _ = rc.LPop("prod:queue:enqueue5").Result()
	err = json.Unmarshal([]byte(bytes), &result)
	assert.NoError(t, err)
	assert.Equal(t, "Compare", result["class"])

	ea := result["enqueued_at"].(float64)
	assert.InDelta(t, nowToSecondsWithNanoPrecision(), ea, 0.1)

	// has retry and retry_count when set
	p.EnqueueWithOptions("enqueue6", "Compare", []string{"foo", "bar"}, EnqueueOptions{RetryCount: 13, Retry: true})

	bytes, _ = rc.LPop("prod:queue:enqueue6").Result()
	err = json.Unmarshal([]byte(bytes), &result)
	assert.NoError(t, err)
	assert.Equal(t, "Compare", result["class"])

	retry := result["retry"].(bool)
	assert.True(t, retry)

	retryCount := int(result["retry_count"].(float64))
	assert.Equal(t, 13, retryCount)
}

func TestProducer_EnqueueIn(t *testing.T) {
	namespace := "prod"
	opts, err := setupTestOptionsWithNamespace(namespace)
	assert.NoError(t, err)
	rc := opts.client
	p := &Producer{opts: opts}

	scheduleQueue := namespace + ":" + scheduledJobsKey

	//has added a job in the scheduled queue
	_, err = p.EnqueueIn("enqueuein1", "Compare", 10, map[string]interface{}{"foo": "bar"})
	assert.NoError(t, err)

	scheduledCount, err := rc.ZCard(scheduleQueue).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), scheduledCount)

	rc.Del(scheduleQueue)

	//has the correct 'queue'
	_, err = p.EnqueueIn("enqueuein2", "Compare", 10, map[string]interface{}{"foo": "bar"})
	assert.NoError(t, err)

	var data EnqueueData
	elem, err := rc.ZRange(scheduleQueue, 0, -1).Result()
	bytes := elem[0]
	err = json.Unmarshal([]byte(bytes), &data)
	assert.NoError(t, err)

	assert.Equal(t, "enqueuein2", data.Queue)

	rc.Del(scheduleQueue)
}

func TestMultipleEnqueueOrder(t *testing.T) {
	namespace := "prod"
	opts, err := setupTestOptionsWithNamespace(namespace)
	assert.NoError(t, err)
	rc := opts.client
	p := &Producer{opts: opts}

	var msg1, _ = NewMsg("{\"key\":\"1\"}")
	_, err = p.Enqueue("testq1", "Compare", msg1.ToJson())
	assert.NoError(t, err)

	var msg2, _ = NewMsg("{\"key\":\"2\"}")
	_, err = p.Enqueue("testq1", "Compare", msg2.ToJson())
	assert.NoError(t, err)

	len, err := rc.LLen("prod:queue:testq1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), len)

	bytesMsg, err := rc.RPop("prod:queue:testq1").Result()
	assert.NoError(t, err)
	var data EnqueueData
	err = json.Unmarshal([]byte(bytesMsg), &data)
	assert.NoError(t, err)
	actualMsg, err := NewMsg(data.Args.(string))
	assert.NoError(t, err)
	assert.Equal(t, msg1.Get("key"), actualMsg.Get("key"))

	bytesMsg, err = rc.RPop("prod:queue:testq1").Result()
	assert.NoError(t, err)
	err = json.Unmarshal([]byte(bytesMsg), &data)
	assert.NoError(t, err)
	actualMsg, err = NewMsg(data.Args.(string))
	assert.NoError(t, err)
	assert.Equal(t, msg2.Get("key"), actualMsg.Get("key"))

	len, err = rc.LLen("prod:queue:testq1").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), len)
}
