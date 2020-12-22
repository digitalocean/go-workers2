package workers

import (
	"reflect"

	"github.com/bitly/go-simplejson"
)

type data struct {
	*simplejson.Json
}

// Msg is the struct for job data (parameters and metadata)
type Msg struct {
	*data
	original  string
	ack       bool
	startedAt int64
}

// Args is the set of parameters for a message
type Args struct {
	*data
}

// Class returns class attribute of a message
func (m *Msg) Class() string {
	return m.Get("class").MustString()
}

// Jid returns job id attribute of a message
func (m *Msg) Jid() string {
	return m.Get("jid").MustString()
}

// Args returns arguments attribute of a message
func (m *Msg) Args() *Args {
	if args, ok := m.CheckGet("args"); ok {
		return &Args{&data{args}}
	}

	d, _ := newData("[]")
	return &Args{d}
}

// OriginalJson returns the original JSON message
func (m *Msg) OriginalJson() string {
	return m.original
}

// ToJson return data in JSON format th message
func (d *data) ToJson() string {
	json, err := d.Encode()

	if err != nil {
		Logger.Println("ERR: Couldn't generate json from", d, ":", err)
	}

	return string(json)
}

func (d *data) Equals(other interface{}) bool {
	otherJSON := reflect.ValueOf(other).MethodByName("ToJson").Call([]reflect.Value{})
	return d.ToJson() == otherJSON[0].String()
}

// NewMsg returns a new message
func NewMsg(content string) (*Msg, error) {
	d, err := newData(content)
	if err != nil {
		return nil, err
	}
	return &Msg{
		data:      d,
		original:  content,
		ack:       true,
		startedAt: 0,
	}, nil
}

func newData(content string) (*data, error) {
	json, err := simplejson.NewJson([]byte(content))
	if err != nil {
		return nil, err
	}
	return &data{json}, nil
}
