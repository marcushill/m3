package promremote

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
)

func TestNewFromConfiguration(t *testing.T) {
	opts, err := NewOptions(config.PrometheusRemoteBackendConfiguration{
		Endpoints: []config.PrometheusRemoteBackendEndpointConfiguration{{
			Address:    "testAddress",
			Resolution: time.Second,
			Retention:  time.Millisecond,
		}},
		RequestTimeout:  time.Nanosecond,
		ConnectTimeout:  time.Microsecond,
		KeepAlive:       time.Millisecond,
		IdleConnTimeout: time.Second,
		MaxIdleConns:    1,
	})
	require.NoError(t, err)
	assert.Equal(t, opts, Options{
		endpoints: []EndpointOptions{{
			address:    "testAddress",
			resolution: time.Second,
			retention:  time.Millisecond,
		}},
		requestTimeout:  time.Nanosecond,
		connectTimeout:  time.Microsecond,
		keepAlive:       time.Millisecond,
		idleConnTimeout: time.Second,
		maxIdleConns:    1,
	})
}

func TestHTTPDefaults(t *testing.T) {
	cfg, err := NewOptions(config.PrometheusRemoteBackendConfiguration{
		Endpoints: []config.PrometheusRemoteBackendEndpointConfiguration{getValidEndpointConfiguration()},
	})
	require.NoError(t, err)
	opts := cfg.HTTPClientOptions()

	assert.Equal(t, 60*time.Second, opts.RequestTimeout)
	assert.Equal(t, 5*time.Second, opts.ConnectTimeout)
	assert.Equal(t, 60*time.Second, opts.KeepAlive)
	assert.Equal(t, 60*time.Second, opts.IdleConnTimeout)
	assert.Equal(t, 100, opts.MaxIdleConns)
	assert.Equal(t, true, opts.DisableCompression)
}

func TestValidation(t *testing.T) {
	t.Run("at least 1 endpoint", func(t *testing.T) {
		cfg := getValidConfig()
		cfg.Endpoints = nil
		assertValidationError(t, cfg, "at least one endpoint must be configured when using prom-remote backend type")
	})

	t.Run("valid endpoint", func(t *testing.T) {
		cfg := getValidConfig()
		cfg.Endpoints[0].Address = ""
		assertValidationError(t, cfg, "endpoint address must be set")
	})

	t.Run("non negative keep alive", func(t *testing.T) {
		cfg := getValidConfig()
		cfg.KeepAlive = -1
		assertValidationError(t, cfg, "keepAlive can't be negative")
	})
	t.Run("non negative max idle conns", func(t *testing.T) {
		cfg := getValidConfig()
		cfg.MaxIdleConns = -1
		assertValidationError(t, cfg, "maxIdleConns can't be negative")
	})

	t.Run("non negative idle conn timeout", func(t *testing.T) {
		cfg := getValidConfig()
		cfg.IdleConnTimeout = -1
		assertValidationError(t, cfg, "idleConnTimeout can't be negative")
	})

	t.Run("non negative request timeout", func(t *testing.T) {
		cfg := getValidConfig()
		cfg.RequestTimeout = -1
		assertValidationError(t, cfg, "requestTimeout can't be negative")
	})

	t.Run("non negative connect timeout", func(t *testing.T) {
		cfg := getValidConfig()
		cfg.ConnectTimeout = -1
		assertValidationError(t, cfg, "connectTimeout can't be negative")
	})
}

func TestValidateEndpoint(t *testing.T) {
	t.Run("address required", func(t *testing.T) {
		cfg := getValidEndpointConfiguration()
		cfg.Address = ""
		assertEndpointError(t, cfg, "endpoint address must be set")
	})

	t.Run("address spaces trimmed", func(t *testing.T) {
		cfg := getValidEndpointConfiguration()
		cfg.Address = "    "
		assertEndpointError(t, cfg, "endpoint address must be set")
	})

	t.Run("retention must be positive", func(t *testing.T) {
		cfg := getValidEndpointConfiguration()
		cfg.Retention = -1
		assertEndpointError(t, cfg, "endpoint retention can't be negative")
	})

	t.Run("resolution must be positive", func(t *testing.T) {
		cfg := getValidEndpointConfiguration()
		cfg.Resolution = -1
		assertEndpointError(t, cfg, "endpoint resolution can't be negative")
	})
}

func assertValidationError(t *testing.T, cfg config.PrometheusRemoteBackendConfiguration, expectedMsg string) {
	_, err := NewOptions(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedMsg)
}

func assertEndpointError(
	t *testing.T,
	cfg config.PrometheusRemoteBackendEndpointConfiguration,
	expectedMsg string,
) {
	err := validateEndpointConfiguration(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedMsg)
}

func getValidConfig() config.PrometheusRemoteBackendConfiguration {
	return config.PrometheusRemoteBackendConfiguration{
		Endpoints: []config.PrometheusRemoteBackendEndpointConfiguration{getValidEndpointConfiguration()},
	}
}

func getValidEndpointConfiguration() config.PrometheusRemoteBackendEndpointConfiguration {
	return config.PrometheusRemoteBackendEndpointConfiguration{
		Address:    "testAddress",
		Retention:  time.Second,
		Resolution: time.Second,
	}
}