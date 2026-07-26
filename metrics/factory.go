// Copyright 2024 Block, Inc.

package metrics

import (
	"fmt"
	"sort"
	"sync"

	"github.com/cashapp/blip"
	"github.com/cashapp/blip/metrics/autoinc"
	awsrds "github.com/cashapp/blip/metrics/aws.rds"
	errordomain "github.com/cashapp/blip/metrics/error"
	"github.com/cashapp/blip/metrics/innodb"
	innodbbufferpool "github.com/cashapp/blip/metrics/innodb.buffer-pool"
	"github.com/cashapp/blip/metrics/percona"
	queryresponsetime "github.com/cashapp/blip/metrics/query.response-time"
	"github.com/cashapp/blip/metrics/repl"
	repllag "github.com/cashapp/blip/metrics/repl.lag"
	sizebinlog "github.com/cashapp/blip/metrics/size.binlog"
	sizedatabase "github.com/cashapp/blip/metrics/size.database"
	sizetable "github.com/cashapp/blip/metrics/size.table"
	statusglobal "github.com/cashapp/blip/metrics/status.global"
	"github.com/cashapp/blip/metrics/stmt.current"
	"github.com/cashapp/blip/metrics/tls"
	"github.com/cashapp/blip/metrics/trx"
	varglobal "github.com/cashapp/blip/metrics/var.global"
	waitiotable "github.com/cashapp/blip/metrics/wait.io.table"
)

// Register registers a factory that makes one or more collectors by domain name.
// This is one of several integration points because it allows users
// to plug in new metric collectors by providing a factory to make them.
// Blip calls this function in an init function to register the built-in metric
// collectors.
//
// If the factory implements blip.CollectorFactoryDatabaseTypes, Blip records
// the database types it declares for this domain. Existing factories that do
// not implement that optional interface retain Blip's historical MySQL
// behavior.
//
// See types in the blip package for more details.
func Register(domain string, f blip.CollectorFactory) error {
	r.Lock()
	_, registered := r.factory[domain]
	r.Unlock()
	if registered {
		return fmt.Errorf("%s already registered", domain)
	}

	databaseTypes := []blip.DatabaseType{blip.DatabaseTypeMySQL}
	if typedFactory, ok := f.(blip.CollectorFactoryDatabaseTypes); ok {
		databaseTypes = typedFactory.DatabaseTypes(domain)
	}
	databaseTypes, err := normalizeDatabaseTypes(domain, databaseTypes)
	if err != nil {
		return err
	}

	r.Lock()
	defer r.Unlock()
	// Another goroutine might have registered the domain while the factory's
	// optional compatibility metadata was being evaluated.
	if _, registered := r.factory[domain]; registered {
		return fmt.Errorf("%s already registered", domain)
	}
	r.factory[domain] = registeredFactory{
		factory:       f,
		databaseTypes: databaseTypes,
	}
	blip.Debug("register collector %s for database types %v", domain, databaseTypes)
	return nil
}

func normalizeDatabaseTypes(domain string, databaseTypes []blip.DatabaseType) ([]blip.DatabaseType, error) {
	if len(databaseTypes) == 0 {
		return nil, fmt.Errorf("collector %s supports no database types", domain)
	}

	seen := map[blip.DatabaseType]bool{}
	normalized := make([]blip.DatabaseType, 0, len(databaseTypes))
	for _, databaseType := range databaseTypes {
		switch databaseType {
		case blip.DatabaseTypeMySQL, blip.DatabaseTypePostgres:
		default:
			return nil, fmt.Errorf("collector %s declares invalid database type %q", domain, databaseType)
		}
		if seen[databaseType] {
			continue
		}
		seen[databaseType] = true
		normalized = append(normalized, databaseType)
	}
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i] < normalized[j]
	})
	return normalized, nil
}

// Remove removes the metrics collector factory for the given domain. This is
// used for testing, but it can also be used to remove (or override) built-in
// metric collectors.
func Remove(domain string) {
	r.Lock()
	defer r.Unlock()
	delete(r.factory, domain)
	blip.Debug("removed collector %s", domain)
}

// List lists all registered metric collectors. It is used by the server API
// for GET /registered.
func List() []string {
	r.Lock()
	defer r.Unlock()
	names := []string{}
	for k := range r.factory {
		names = append(names, k)
	}
	return names
}

// Exists returns true if a collector for the domain has been registered.
func Exists(domain string) bool {
	r.Lock()
	defer r.Unlock()
	_, ok := r.factory[domain]
	return ok
}

// SupportedDatabaseTypes returns a copy of the database types supported by the
// registered collector domain.
func SupportedDatabaseTypes(domain string) ([]blip.DatabaseType, error) {
	r.Lock()
	defer r.Unlock()
	registered, ok := r.factory[domain]
	if !ok {
		return nil, fmt.Errorf("invalid domain: %s (no factory registered)", domain)
	}
	databaseTypes := make([]blip.DatabaseType, len(registered.databaseTypes))
	copy(databaseTypes, registered.databaseTypes)
	return databaseTypes, nil
}

// ValidateDatabase returns nil if the domain exists and can be used with the
// database type.
func ValidateDatabase(domain string, databaseType blip.DatabaseType) error {
	r.Lock()
	defer r.Unlock()
	return validateDatabase(domain, databaseType)
}

func validateDatabase(domain string, databaseType blip.DatabaseType) error {
	registered, ok := r.factory[domain]
	if !ok {
		return fmt.Errorf("invalid domain: %s (no factory registered)", domain)
	}
	for _, supportedType := range registered.databaseTypes {
		if supportedType == databaseType {
			return nil
		}
	}
	return fmt.Errorf("collector %s does not support database type %q (supported: %v)",
		domain, databaseType, registered.databaseTypes)
}

// Make makes a metric collector for the domain using a previously registered factory.
//
// See types in the blip package for more details.
func Make(domain string, args blip.CollectorFactoryArgs) (blip.Collector, error) {
	return makeWithDBProvider(domain, args, nil)
}

// MakeWithDBProvider makes a collector with an optional monitor-owned database
// provider. Registered factories without the optional provider capability
// continue through their historical Make method.
func MakeWithDBProvider(domain string, args blip.CollectorFactoryArgs, provider blip.DbProvider) (blip.Collector, error) {
	return makeWithDBProvider(domain, args, provider)
}

func makeWithDBProvider(domain string, args blip.CollectorFactoryArgs, provider blip.DbProvider) (blip.Collector, error) {
	r.Lock()
	defer r.Unlock()
	registered, ok := r.factory[domain]
	if !ok {
		return nil, fmt.Errorf("invalid domain: %s (no factory registered)", domain)

	}
	// ValidatePlans creates collectors without a monitor. Database compatibility
	// is checked when the monitor resolves the selected plan.
	if !args.Validate {
		if err := validateDatabase(domain, args.Config.EffectiveDatabaseType()); err != nil {
			return nil, err
		}
	}
	if provider != nil {
		if providerFactory, ok := registered.factory.(blip.CollectorFactoryWithDBProvider); ok {
			return providerFactory.MakeWithDBProvider(domain, args, provider)
		}
	}
	return registered.factory.Make(domain, args)
}

func PrintDomains() string {
	r.Lock()
	domains := make([]string, 0, len(r.factory))
	for d := range r.factory {
		domains = append(domains, d)
	}
	sort.Strings(domains)
	r.Unlock()

	out := ""
	for _, domain := range domains {
		mc, _ := Make(domain, blip.CollectorFactoryArgs{Validate: true})
		help := mc.Help()
		out += fmt.Sprintf("%s\n\t%s\n\n",
			help.Domain, help.Description,
		)

		// Options block
		opts := make([]string, 0, len(help.Options))
		for o := range help.Options {
			opts = append(opts, o)
		}
		if len(opts) > 0 {
			out += "\tOptions:\n"
			sort.Strings(opts)
			for _, optName := range opts {
				optHelp := help.Options[optName]
				out += "\t\t" + optName + ": " + optHelp.Desc
				if len(optHelp.Values) > 0 {
					out += "\n"
					valWidth := 0
					for val := range optHelp.Values {
						if len(val) > valWidth {
							valWidth = len(val)
						}
					}
					valLine := fmt.Sprintf("\t\t| %%-%ds = %%s", valWidth)

					for val, desc := range optHelp.Values {
						out += fmt.Sprintf(valLine, val, desc)
						if val == optHelp.Default {
							out += " (default)"
						}
						out += "\n"
					}
					out += "\n"
				} else if optHelp.Default != "" {
					out += " (default: " + optHelp.Default + ")\n\n"
				} else {
					out += "\n\n"
				}
			}
		} else {
			out += "\t(No options)\n\n"
		}

		// Errors block
		errs := make([]string, 0, len(help.Errors))
		for e := range help.Errors {
			errs = append(errs, e)
		}
		if len(errs) > 0 {
			out += "\tErrors:\n"
			sort.Strings(errs)
			for _, errName := range errs {
				optHelp := help.Errors[errName]
				out += "\t\t" + errName + ": " + optHelp.Handles + "\n"
			}
			out += "\n"
		}

		if len(help.Groups) > 0 {
			out += "\tGroups:\n"
			for _, kv := range help.Groups {
				out += "\t\t" + kv.Key + " = " + kv.Value + "\n"
			}
			out += "\n"
		}

		if len(help.Meta) > 0 {
			out += "\tMeta:\n"
			for _, kv := range help.Meta {
				out += "\t\t" + kv.Key + " = " + kv.Value + "\n"
			}
			out += "\n"
		}

		if len(help.Metrics) > 0 {
			out += "\tMetrics:\n"
			for _, m := range help.Metrics {
				out += "\t\t" + m.Name
				switch m.Type {
				case blip.CUMULATIVE_COUNTER:
					out += " (cumulative counter)"
				case blip.DELTA_COUNTER:
					out += " (delta counter)"
				case blip.GAUGE:
					out += " (gauge)"
				default:
					out += " (unknown type)"
				}
				out += ": " + m.Desc + "\n"
			}
			out += "\n"
		}

		out += "\n"
	}

	return out
}

// --------------------------------------------------------------------------

// Register built-in collectors using built-in factories.
func init() {
	for _, mc := range builtinCollectors {
		Register(mc, f)
	}
}

// repo holds registered blip.CollectorFactory. There's a single package
// instance below.
type repo struct {
	*sync.Mutex
	factory map[string]registeredFactory
}

type registeredFactory struct {
	factory       blip.CollectorFactory
	databaseTypes []blip.DatabaseType
}

// Internal package instance of repo that holds all collector factories registered
// by calls to Register, which includes the built-in factories.
var r = &repo{
	Mutex:   &sync.Mutex{},
	factory: map[string]registeredFactory{},
}

// factory is the built-in factory for creating all built-in collectors.
// There's a single package instance below. It implements blip.CollectorFactory.
type factory struct {
	AWSConfig  blip.AWSConfigFactory
	HTTPClient blip.HTTPClientFactory
}

var _ blip.CollectorFactory = &factory{}
var _ blip.CollectorFactoryDatabaseTypes = &factory{}

// Internet package instance of factory that makes all built-it collectors.
// This factory is registered in the init func above.
var f = &factory{}

func InitFactory(factories blip.Factories) {
	f.AWSConfig = factories.AWSConfig
	f.HTTPClient = factories.HTTPClient
}

func (f *factory) DatabaseTypes(domain string) []blip.DatabaseType {
	if domain == awsrds.DOMAIN {
		return []blip.DatabaseType{
			blip.DatabaseTypeMySQL,
			blip.DatabaseTypePostgres,
		}
	}
	return []blip.DatabaseType{blip.DatabaseTypeMySQL}
}

// Make makes a metric collector for the domain. This is the built-in factory
// that makes the built-in collectors: status.global, var.global, and so on.
func (f *factory) Make(domain string, args blip.CollectorFactoryArgs) (blip.Collector, error) {
	switch domain {
	case "autoinc":
		return autoinc.NewAutoInc(args.DB), nil
	case "aws.rds":
		if args.Validate {
			return awsrds.NewRDS(nil), nil
		}
		region := args.Config.AWS.Region
		if region == "" && !blip.True(args.Config.AWS.DisableAutoRegion) {
			region = "auto"
		}
		awsConfig, err := f.AWSConfig.Make(blip.AWS{Region: region}, args.Config.Hostname)
		if err != nil {
			return nil, err
		}
		return awsrds.NewRDS(awsrds.NewCloudWatchClient(awsConfig)), nil
	case "error.account":
		return errordomain.NewErrorAccount(args.DB), nil
	case "error.global":
		return errordomain.NewErrorGlobal(args.DB), nil
	case "error.host":
		return errordomain.NewErrorHost(args.DB), nil
	case "error.thread":
		return errordomain.NewErrorThread(args.DB), nil
	case "error.user":
		return errordomain.NewErrorUser(args.DB), nil
	case "innodb":
		return innodb.NewInnoDB(args.DB), nil
	case "innodb.buffer-pool":
		return innodbbufferpool.NewBufferPoolStats(args.DB), nil
	case "percona.response-time":
		return percona.NewQRT(args.DB), nil
	case "query.response-time":
		return queryresponsetime.NewResponseTime(args.DB), nil
	case "repl":
		return repl.NewRepl(args.DB), nil
	case "repl.lag":
		return repllag.NewLag(args.DB), nil
	case "size.binlog":
		return sizebinlog.NewBinlog(args.DB), nil
	case "size.database":
		return sizedatabase.NewDatabase(args.DB), nil
	case "size.table":
		return sizetable.NewTable(args.DB), nil
	case "status.global":
		return statusglobal.NewGlobal(args.DB), nil
	case "stmt.current":
		return stmt.NewCurrent(args.DB), nil
	case "tls":
		return tls.NewTLS(args.DB), nil
	case "trx":
		return trx.NewTrx(args.DB), nil
	case "var.global":
		return varglobal.NewGlobal(args.DB), nil
	case "wait.io.table":
		return waitiotable.NewTable(args.DB), nil
	}
	return nil, fmt.Errorf("invalid domain: %s", domain)
}

// List of built-in collectors. To add one, add its domain name here, and add
// the same domain in the switch statement above (in factory.Make).
var builtinCollectors = []string{
	"autoinc",
	"aws.rds",
	"error.account",
	"error.global",
	"error.host",
	"error.thread",
	"error.user",
	"innodb",
	"innodb.buffer-pool",
	"percona.response-time",
	"query.response-time",
	"repl",
	"repl.lag",
	"size.binlog",
	"size.database",
	"size.table",
	"status.global",
	"stmt.current",
	"trx",
	"tls",
	"var.global",
	"wait.io.table",
}
