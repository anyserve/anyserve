package meta

type engine interface {
	doInit(format *Format, force bool) error
}
