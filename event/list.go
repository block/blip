// Copyright 2024 Block, Inc.

package event

// Blip events (non-monitor)
const (
	BOOT_CONFIG_INVALID   = "boot-config-invalid"
	BOOT_CONFIG_LOADED    = "boot-config-loaded"
	BOOT_CONFIG_LOADING   = "boot-config-loading"
	BOOT_ERROR            = "boot-error"
	BOOT_START            = "boot-start"
	BOOT_SUCCESS          = "boot-success"
	MONITORS_LOADED       = "monitors-loaded"
	MONITORS_LOADING      = "monitors-loading"
	MONITORS_RELOAD_ERROR = "monitors-reload-error"
	MONITORS_STARTED      = "monitors-started"
	MONITORS_STARTING     = "monitors-starting"
	MONITORS_STOPLOSS     = "monitors-stoploss"
	MONITOR_LOADER_PANIC  = "monitor-loader-panic"
	PLANS_LOAD_MONITOR    = "plans-load-monitor"
	PLANS_LOAD_SHARED     = "plans-load-shared"
	SERVER_API_PANIC      = "server-api-panic"
	SERVER_API_ERROR      = "server-api-error"
	SERVER_RUN            = "server-run"
	SERVER_STOPPED        = "server-stopped"
)

// Monitor events
const (
	CHANGE_PLAN              = "change-plan"
	CHANGE_PLAN_ERROR        = "change-plan-error"
	CHANGE_PLAN_SUCCESS      = "change-plan-success"
	DB_RELOAD_PASSWORD_ERROR = "db-reload-password-error"
	ENGINE_COLLECT_ERROR     = "engine-collect-error"
	ENGINE_PREPARE           = "engine-prepare"
	ENGINE_PREPARE_ERROR     = "engine-prepare-error"
	ENGINE_PREPARE_SUCCESS   = "engine-prepare-success"
	ENGINE_EMR_TIMEOUT       = "engine-emr-timeout"
	LCO_COLLECT_PANIC        = "lco-collect-panic"
	LCO_RECEIVER_PANIC       = "lco-receiver-panic"
	LCO_PAUSED               = "lco-paused"
	LCO_RUNNING              = "lco-running"
	LCO_METRICS_FAULT        = "lco-metrics-fault"
	MONITOR_CONNECTED        = "connected"
	MONITOR_CONNECTING       = "connecting"
	MONITOR_ERROR            = "monitor-error"
	MONITOR_PANIC            = "monitor-panic"
	MONITOR_STARTED          = "monitor-started"
	MONITOR_STOPPED          = "monitor-stopped"
	MONITOR_STOP_TIMEOUT     = "monitor-stop-timeout"
	STATE_CHANGE_ABORT       = "state-change-abort"
	STATE_CHANGE_BEGIN       = "state-change-begin"
	STATE_CHANGE_END         = "state-change-end"
	REPL_SOURCE_CHANGE       = "repl-soruce-change"
)

const (
	COLLECTOR_CMR_TIMEOUT = "collector-cmr-timeout"
	COLLECTOR_ERROR       = "collector-error"
	COLLECTOR_PANIC       = "collector-panic"
	COLLECTOR_FAULT       = "collector-fault"
	DROP_METRICS_FENCE    = "drop-metrics-fence"   // behind fence due to collector fault
	DROP_METRICS_FLUSH    = "drop-metrics-flush"   // Engine.collectionChan <- collection{} blocked
	DROP_METRICS_RUNTIME  = "drop-metrics-runtime" // @todo collection.runtime > user-configured timeout
)

// Sink Events
const (
	SINK_INVALID_METRICS = "sink-invalid-metrics" // invalid metrics, drop
	SINK_PAYLOAD         = "sink-payload"         // payload sizing and submission diagnostics
	SINK_SERVER_ERROR    = "sink-server-error"    // send ok but remote server returned an error
	SINK_SEND_ERROR      = "sink-send-error"      // e.g. network timeout
)
