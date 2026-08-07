// Copyright 2024 Block, Inc.

package tr

import (
	"fmt"
	"sync"

	"github.com/cashapp/blip/v2"
)

type DomainTranslator interface {
	Translate(domain, metric string) string
}

func Register(name string, tr DomainTranslator) error {
	r.Lock()
	defer r.Unlock()
	_, ok := r.tr[name]
	if ok {
		return fmt.Errorf("domain translator %s already registered", name)
	}
	r.tr[name] = tr
	blip.Debug("register domain tr: %s", name)
	return nil
}

// Exists returns true if a collector for the name has been registered.
func Exists(name string) bool {
	r.Lock()
	defer r.Unlock()
	_, ok := r.tr[name]
	return ok
}

func Make(name string) (DomainTranslator, error) {
	r.Lock()
	defer r.Unlock()
	tr, ok := r.tr[name]
	if !ok {
		return nil, fmt.Errorf("invalid domain translator: %s (not registered)", name)
	}
	return tr, nil
}

// repo holds registered blip.CollectorFactory. There's a single package
// instance below.
type repo struct {
	*sync.Mutex
	tr map[string]DomainTranslator
}

// Internal package instance of repo that holds all collector factories registered
// by calls to Register, which includes the built-in factories.
var r = &repo{
	Mutex: &sync.Mutex{},
	tr:    map[string]DomainTranslator{},
}
