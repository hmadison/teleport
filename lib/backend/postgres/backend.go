/*
Copyright 2022 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package postgres

import (
	"context"

	"github.com/gravitational/teleport/api/utils"
	"github.com/gravitational/teleport/lib/backend"
	"github.com/gravitational/teleport/lib/backend/sqlbk"
	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"

	// Ensure pgx driver is registered.
	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	// BackendName is the name of this backend.
	BackendName = "postgres"
	// AlternativeName is another name of this backend.
	AlternativeName = "cockroachdb"
)

// GetName returns BackendName (postgres).
func GetName() string {
	return BackendName
}

// New returns a Backend that speaks the PostgreSQL protocol when communicating
// with the database. A non-nil error means the connection pool is ready and the
// database has been migrated to the most recent version.
func New(ctx context.Context, params backend.Params) (*sqlbk.Backend, error) {
	var cfg *Config
	err := utils.ObjectToStruct(params, &cfg)
	if err != nil {
		return nil, trace.BadParameter("invalid configuration: %v", err)
	}
	err = cfg.CheckAndSetDefaults()
	if err != nil {
		return nil, trace.Wrap(err)
	}
	return sqlbk.New(ctx, &pgDriver{cfg: cfg})
}

// Config defines a configuration for the postgres backend.
type Config struct {
	sqlbk.Config

	// Add configurations specific to this backend.
	//
	// Postgres struct {
	//    AfterConnect pgconn.AfterConnectFunc `json:"-"`
	//    DialFunc     pgconn.DialFunc         `json:"-"`
	//    RuntimeParams struct {
	//      SearchPath string `json:"search_path"`
	//    } `json:"runtime_params"`
	// } `json:"postgres"
}

// CheckAndSetDefaults validates required fields and sets default
// values for fields that have not been set.
func (c *Config) CheckAndSetDefaults() error {
	if c.Log == nil {
		c.Log = logrus.WithFields(logrus.Fields{trace.Component: BackendName})
	}
	if c.Clock == nil {
		c.Clock = clockwork.NewRealClock()
	}
	return c.Config.CheckAndSetDefaults()
}
