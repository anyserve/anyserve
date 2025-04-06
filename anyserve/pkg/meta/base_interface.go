package meta

import (
	"sync"
)

type baseMeta struct {
	sync.Mutex
	format *Format

	en engine
}

func newBaseMeta(addr string) *baseMeta {
	return &baseMeta{}
}

func (m *baseMeta) Init(format *Format, force bool) error {
	return m.en.doInit(format, force)
}

func (m *baseMeta) getFormat() *Format {
	m.Lock()
	defer m.Unlock()
	return m.format
}

func (m *baseMeta) GetFormat() Format {
	return *m.getFormat()
}
