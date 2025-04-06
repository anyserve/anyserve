package meta

import (
	"encoding/json"
	"fmt"
	"sync"
)

// baseMeta is the base implementation of the Meta interface
type baseMeta struct {
	sync.Mutex
	addr   string
	format *Format

	e engine
}

func newBaseMeta(addr string) *baseMeta {
	return &baseMeta{
		addr: addr,
	}
}

func (m *baseMeta) Init(format *Format, force bool) error {
	return m.e.doInit(format, force)
}

func (m *baseMeta) GetFormat() Format {
	m.Lock()
	defer m.Unlock()
	return *m.format
}

func (m *baseMeta) Load() (*Format, error) {
	body, err := m.e.doLoad()
	if err == nil && len(body) == 0 {
		err = fmt.Errorf("backend is not formatted, please run `anyserve init ...` first")
	}
	if err != nil {
		return nil, err
	}
	var format = new(Format)
	if err = json.Unmarshal(body, format); err != nil {
		return nil, fmt.Errorf("json: %s", err)
	}
	m.Lock()
	m.format = format
	m.Unlock()
	return format, nil
}
