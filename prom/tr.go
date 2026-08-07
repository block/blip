// Copyright 2024 Block, Inc.

package prom

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/prom/tr"
)

type DomainTranslator interface {
	Translate(values []blip.MetricValue, ch chan<- prometheus.Metric)

	Names() (prefix, domain, shortDomin string)
}

var mu = &sync.Mutex{}

func Register(blipDomain string, tr DomainTranslator) error {
	mu.Lock()
	defer mu.Unlock()
	trRepo[blipDomain] = tr
	return nil
}

func Translator(domain string) DomainTranslator {
	mu.Lock()
	defer mu.Unlock()
	return trRepo[domain]
}

var trRepo = map[string]DomainTranslator{
	"status.global": tr.StatusGlobal{Domain: "global_status", ShortDomain: "status"},
	"var.global":    tr.Generic{Domain: "global_variables", ShortDomain: "var"},
	"innodb":        tr.InnoDBMetrics{Domain: "info_schema_innodb", ShortDomain: "innodb"},
}
