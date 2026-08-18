// Copyright 2026 Block, Inc.

package aws

import (
	"crypto/tls"
	"crypto/x509"
	_ "embed"
	"errors"
	"sync"

	"github.com/go-sql-driver/mysql"

	"github.com/cashapp/blip/v2"
)

// rdsGlobalBundle is the AWS RDS trust bundle for all commercial regions.
// Source: https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
//
//go:embed rds-global-bundle.pem
var rdsGlobalBundle []byte

var registerRDSCAOnce sync.Once

// NewRDSCAPool returns an independent certificate pool containing the embedded
// AWS RDS trust bundle. Callers can safely customize the returned pool without
// changing the trust roots used by other database connections.
func NewRDSCAPool() (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(rdsGlobalBundle) {
		return nil, errors.New("embedded AWS RDS CA bundle contains no certificates")
	}
	return pool, nil
}

// RegisterRDSCA registers the Amazon RDS certificate authority (CA) to enable
// MySQL TLS connections to RDS. The TLS parameter is called "rds". Registration
// happens only once, but calling RegisterRDSCA multiple times is safe.
func RegisterRDSCA() {
	registerRDSCAOnce.Do(func() {
		blip.Debug("loading RDS CA")
		pool, err := NewRDSCAPool()
		if err != nil {
			panic(err)
		}
		if err := mysql.RegisterTLSConfig("rds", &tls.Config{RootCAs: pool}); err != nil {
			panic(err)
		}
	})
}
