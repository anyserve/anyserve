package meta

// engine define the operations of the meta engine
type engine interface {
	doInit(format *Format, force bool) error
	doLoad() ([]byte, error)
}
