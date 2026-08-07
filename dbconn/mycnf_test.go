// Copyright 2024 Block, Inc.

package dbconn_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cashapp/blip/v2"
	"github.com/cashapp/blip/v2/dbconn"
)

func TestParseMyCnf(t *testing.T) {
	gotCfg, gotTLS, err := dbconn.ParseMyCnf("../test/mycnf/full-dsn")
	if err != nil {
		t.Error(err)
	}
	expectCfg := blip.ConfigMySQL{
		Username: "U",
		Password: "P",
		Hostname: "H:33560",
	}
	expectTLS := blip.ConfigTLS{
		CA:        "CA",
		Cert:      "CERT",
		Key:       "KEY",
		MySQLMode: "PREFERRED",
	}
	assert.Equal(t, expectCfg, gotCfg)
	assert.Equal(t, expectTLS, gotTLS)
}

func TestParseMyCnfSocketTLS(t *testing.T) {
	gotCfg, gotTLS, err := dbconn.ParseMyCnf("../test/mycnf/socket-tls")
	if err != nil {
		t.Error(err)
	}
	expectCfg := blip.ConfigMySQL{
		Username: "U",
		Password: "P",
		Socket:   "socketFile",
	}
	expectTLS := blip.ConfigTLS{
		MySQLMode: "PREFERRED",
	}
	assert.Equal(t, expectCfg, gotCfg)
	assert.Equal(t, expectTLS, gotTLS)
}

func TestParseMyCnfTLSVerifyCA(t *testing.T) {
	gotCfg, gotTLS, err := dbconn.ParseMyCnf("../test/mycnf/tls-verify-ca")
	if err != nil {
		t.Error(err)
	}
	expectCfg := blip.ConfigMySQL{
		Username: "root",
		Password: "foo",
	}
	expectTLS := blip.ConfigTLS{
		CA:        "CA",
		Cert:      "CRT",
		Key:       "KEY",
		MySQLMode: "VERIFY_CA",
	}
	assert.Equal(t, expectCfg, gotCfg)
	assert.Equal(t, expectTLS, gotTLS)
}
