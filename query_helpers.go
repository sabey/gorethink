package gorethink

import (
	p "sabey.co/gorethink/ql2"
)

func newStopQuery(token int64) Query {
	return Query{
		Type:  p.Query_STOP,
		Token: token,
		Opts: map[string]interface{}{
			"noreply": true,
		},
	}
}
