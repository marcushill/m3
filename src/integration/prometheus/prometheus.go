// Copyright (c) 2021  Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Package prometheus contains resources for starting a docker-backed
// Prometheus.
package prometheus

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/integration/resources/docker"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/headers"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	// TestPrometheusDBNodeConfig is the test config for the dbnode.
	TestPrometheusDBNodeConfig = `
db: {}
`

	// TestPrometheusCoordinatorConfig is the test config for the coordinator.
	TestPrometheusCoordinatorConfig = `
limits:
  perQuery:
    maxFetchedSeries: 100

query:
  restrictTags:
    match:
      - name: restricted_metrics_type
        type: NOTEQUAL
        value: hidden
    strip:
    - restricted_metrics_type

lookbackDuration: 10m
`
)

// TODO: extract query limit and timeout status code as params to RunTest

// RunTest contains the logic for running the prometheus test.
func RunTest(t *testing.T, m3 resources.M3Resources, prom resources.ExternalResources) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	logger.Info("running prometheus tests")

	p := prom.(*docker.Prometheus)

	testPrometheusRemoteRead(t, p, logger)
	testPrometheusRemoteWriteMultiNamespaces(t, p, logger)
	testPrometheusRemoteWriteEmptyLabelNameReturns400(t, m3.Coordinator(), logger)
	testPrometheusRemoteWriteEmptyLabelValueReturns400(t, m3.Coordinator(), logger)
	testPrometheusRemoteWriteDuplicateLabelReturns400(t, m3.Coordinator(), logger)
	testPrometheusRemoteWriteTooOldReturns400(t, m3.Coordinator(), logger)
	testPrometheusRemoteWriteRetrictMetricsType(t, m3.Coordinator(), logger)
	testQueryLookbackApplied(t, m3.Coordinator(), logger)
	testQueryLimitsApplied(t, m3.Coordinator(), logger)
	testQueryRestrictMetricsType(t, m3.Coordinator(), logger)
	testQueryTimeouts(t, m3.Coordinator(), logger)
	testPrometheusQueryNativeTimeout(t, m3.Coordinator(), logger)
	testQueryRestrictTags(t, m3.Coordinator(), logger)
	testPrometheusRemoteWriteMapTags(t, m3.Coordinator(), logger)
	testSeries(t, m3.Coordinator(), logger)
	testLabelQueryLimitsApplied(t, m3.Coordinator(), logger)
	testLabels(t, m3.Coordinator(), logger)
}

func testPrometheusRemoteRead(t *testing.T, p *docker.Prometheus, logger *zap.Logger) {
	// Ensure Prometheus can proxy a Prometheus query
	logger.Info("testing prometheus remote read")
	verifyPrometheusQuery(t, p, "prometheus_remote_storage_samples_total", 100)
}

func testPrometheusRemoteWriteMultiNamespaces(
	t *testing.T,
	p *docker.Prometheus,
	logger *zap.Logger,
) {
	logger.Info("testing remote write to multiple namespaces")

	// Make sure we're proxying writes to the unaggregated namespace
	query := fmt.Sprintf(
		"database_write_tagged_success{namespace=\"%v\"}", resources.UnaggName,
	)
	verifyPrometheusQuery(t, p, query, 0)

	// Make sure we're proxying writes to the aggregated namespace
	query = fmt.Sprintf(
		"database_write_tagged_success{namespace=\"%v\"}", resources.AggName,
	)
	verifyPrometheusQuery(t, p, query, 0)
}

func testPrometheusRemoteWriteEmptyLabelNameReturns400(
	t *testing.T,
	coordinator resources.Coordinator,
	logger *zap.Logger,
) {
	logger.Info("test write empty name for a label returns HTTP 400")
	err := coordinator.WriteProm("foo_metric", map[string]string{
		"non_empty_name": "foo",
		"":               "bar",
	}, []prompb.Sample{
		{
			Value:     42,
			Timestamp: storage.TimeToPromTimestamp(xtime.Now()),
		},
	}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "400")
}

func testPrometheusRemoteWriteEmptyLabelValueReturns400(
	t *testing.T,
	coordinator resources.Coordinator,
	logger *zap.Logger,
) {
	logger.Info("test write empty value for a label returns HTTP 400")
	err := coordinator.WriteProm("foo_metric", map[string]string{
		"foo":            "bar",
		"non_empty_name": "",
	}, []prompb.Sample{
		{
			Value:     42,
			Timestamp: storage.TimeToPromTimestamp(xtime.Now()),
		},
	}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "400")
}

func testPrometheusRemoteWriteDuplicateLabelReturns400(
	t *testing.T,
	coordinator resources.Coordinator,
	logger *zap.Logger,
) {
	logger.Info("test write with duplicate labels returns HTTP 400")
	err := coordinator.WritePromWithLabels("foo_metric", []prompb.Label{
		{
			Name:  []byte("dupe_name"),
			Value: []byte("foo"),
		},
		{
			Name:  []byte("non_dupe_name"),
			Value: []byte("bar"),
		},
		{
			Name:  []byte("dupe_name"),
			Value: []byte("baz"),
		},
	}, []prompb.Sample{
		{
			Value:     42,
			Timestamp: storage.TimeToPromTimestamp(xtime.Now()),
		},
	}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "400")
}

func testPrometheusRemoteWriteTooOldReturns400(
	t *testing.T,
	coordinator resources.Coordinator,
	logger *zap.Logger,
) {
	logger.Info("test write into the past returns HTTP 400")
	err := coordinator.WriteProm("foo_metric", nil, []prompb.Sample{
		{
			Value:     3.142,
			Timestamp: storage.TimeToPromTimestamp(xtime.Now().Add(-1 * time.Hour)),
		},
	}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "400")
}

func testPrometheusRemoteWriteRetrictMetricsType(
	t *testing.T,
	coordinator resources.Coordinator,
	logger *zap.Logger,
) {
	logger.Info("test write with unaggregated metrics type works as expected")
	err := coordinator.WriteProm("bar_metric", nil, []prompb.Sample{
		{
			Value:     42.42,
			Timestamp: storage.TimeToPromTimestamp(xtime.Now()),
		},
	}, resources.Headers{
		headers.MetricsTypeHeader: []string{"unaggregated"},
	})
	require.NoError(t, err)

	logger.Info("test write with aggregated metrics type works as expected")
	err = coordinator.WriteProm("bar_metric", nil, []prompb.Sample{
		{
			Value:     84.84,
			Timestamp: storage.TimeToPromTimestamp(xtime.Now()),
		},
	}, resources.Headers{
		headers.MetricsTypeHeader:          []string{"aggregated"},
		headers.MetricsStoragePolicyHeader: []string{"15s:6h"},
	})
	require.NoError(t, err)
}

func testQueryLookbackApplied(
	t *testing.T,
	coordinator resources.Coordinator,
	logger *zap.Logger,
) {
	// NB: this test depends on the config in m3coordinator.yml for this test
	// and the following config value "lookbackDuration: 10m".
	logger.Info("test lookback config respected")

	err := coordinator.WriteProm("lookback_test", nil, []prompb.Sample{
		{
			Value:     42.42,
			Timestamp: storage.TimeToPromTimestamp(xtime.Now().Add(-9 * time.Minute)),
		},
	}, resources.Headers{
		headers.MetricsTypeHeader: []string{"unaggregated"},
	})
	require.NoError(t, err)

	requireRangeQuerySuccess(t,
		coordinator,
		resources.RangeQueryRequest{
			Query: "lookback_test",
			Start: time.Now().Add(-10 * time.Minute),
			End:   time.Now(),
			Step:  15 * time.Second,
		},
		nil,
		func(res model.Matrix) error {
			if len(res) == 0 || len(res[0].Values) == 0 {
				return errors.New("no samples found")
			}

			latestTS := res[0].Values[len(res[0].Values)-1].Timestamp.Time()
			nowMinusTwoSteps := time.Now().Add(-30 * time.Second)
			if latestTS.After(nowMinusTwoSteps) {
				return nil
			}

			return errors.New("latest timestamp is not within two steps from now")
		},
	)
}

func testQueryLimitsApplied(
	t *testing.T,
	coordinator resources.Coordinator,
	logger *zap.Logger,
) {
	logger.Info("test query series limit with coordinator limit header " +
		"(default errors without RequireExhaustive disabled)")
	requireError(t, func() error {
		_, err := coordinator.InstantQuery(resources.QueryRequest{
			Query: "{metrics_storage=\"m3db_remote\"}",
		}, resources.Headers{
			headers.LimitMaxSeriesHeader: []string{"10"},
		})
		return err
	}, "query exceeded limit")

	logger.Info("test query series limit with require-exhaustive headers false")
	requireInstantQuerySuccess(t,
		coordinator,
		resources.QueryRequest{
			Query: "database_write_tagged_success",
		},
		resources.Headers{
			headers.LimitMaxSeriesHeader:         []string{"2"},
			headers.LimitRequireExhaustiveHeader: []string{"false"},
		},
		func(res model.Vector) error {
			if len(res) != 2 {
				return fmt.Errorf("expected two results. received %d", len(res))
			}

			return nil
		})

	logger.Info("test query series limit with require-exhaustive headers true " +
		"(below limit therefore no error)")
	requireInstantQuerySuccess(t,
		coordinator,
		resources.QueryRequest{
			Query: "database_write_tagged_success",
		},
		resources.Headers{
			headers.LimitMaxSeriesHeader:         []string{"4"},
			headers.LimitRequireExhaustiveHeader: []string{"true"},
		},
		func(res model.Vector) error {
			if len(res) != 3 {
				return fmt.Errorf("expected three results. received %d", len(res))
			}

			return nil
		})

	logger.Info("test query series limit with require-exhaustive headers " +
		"true (above limit therefore error)")
	requireError(t, func() error {
		_, err := coordinator.InstantQuery(resources.QueryRequest{
			Query: "database_write_tagged_success",
		}, resources.Headers{
			headers.LimitMaxSeriesHeader:         []string{"3"},
			headers.LimitRequireExhaustiveHeader: []string{"true"},
		})
		return err
	}, "query exceeded limit")

	requireError(t, func() error {
		_, err := coordinator.InstantQuery(resources.QueryRequest{
			Query: "database_write_tagged_success",
		}, resources.Headers{
			headers.LimitMaxSeriesHeader:         []string{"3"},
			headers.LimitRequireExhaustiveHeader: []string{"true"},
		})
		return err
	}, "400")

	logger.Info("test query docs limit with require-exhaustive headers false")
	requireInstantQuerySuccess(t,
		coordinator,
		resources.QueryRequest{
			Query: "database_write_tagged_success",
		},
		resources.Headers{
			headers.LimitMaxDocsHeader:           []string{"1"},
			headers.LimitRequireExhaustiveHeader: []string{"false"},
		},
		func(res model.Vector) error {
			// NB(nate): docs limit is imprecise so will not match exact number of series
			// returned
			if len(res) != 3 {
				return fmt.Errorf("expected three results. received %d", len(res))
			}

			return nil
		})

	logger.Info("test query docs limit with require-exhaustive headers true " +
		"(below limit therefore no error)")
	requireInstantQuerySuccess(t,
		coordinator,
		resources.QueryRequest{
			Query: "database_write_tagged_success",
		},
		resources.Headers{
			headers.LimitMaxDocsHeader:           []string{"4"},
			headers.LimitRequireExhaustiveHeader: []string{"true"},
		},
		func(res model.Vector) error {
			// NB(nate): docs limit is imprecise so will not match exact number of series
			// returned
			if len(res) != 3 {
				return fmt.Errorf("expected three results. received %d", len(res))
			}

			return nil
		})

	logger.Info("test query docs limit with require-exhaustive headers " +
		"true (above limit therefore error)")
	requireError(t, func() error {
		_, err := coordinator.InstantQuery(resources.QueryRequest{
			Query: "database_write_tagged_success",
		}, resources.Headers{
			headers.LimitMaxDocsHeader:           []string{"1"},
			headers.LimitRequireExhaustiveHeader: []string{"true"},
		})
		return err
	}, "query exceeded limit")

	requireError(t, func() error {
		_, err := coordinator.InstantQuery(resources.QueryRequest{
			Query: "database_write_tagged_success",
		}, resources.Headers{
			headers.LimitMaxDocsHeader:           []string{"1"},
			headers.LimitRequireExhaustiveHeader: []string{"true"},
		})
		return err
	}, "400")

	logger.Info("test query returned datapoints limit - zero limit disabled")
	requireRangeQuerySuccess(t,
		coordinator,
		resources.RangeQueryRequest{
			Query: "database_write_tagged_success",
			Start: time.Now().Add(-100 * time.Minute),
			End:   time.Now(),
			Step:  15 * time.Second,
		},
		resources.Headers{
			headers.LimitMaxReturnedDatapointsHeader: []string{"0"},
		},
		func(res model.Matrix) error {
			if len(res) != 3 {
				return fmt.Errorf("expected three results. received %d", len(res))
			}

			return nil
		})

	logger.Info("test query returned series limit - zero limit disabled")
	requireRangeQuerySuccess(t,
		coordinator,
		resources.RangeQueryRequest{
			Query: "database_write_tagged_success",
			Start: time.Now().Add(-100 * time.Minute),
			End:   time.Now(),
			Step:  15 * time.Second,
		},
		resources.Headers{
			headers.LimitMaxReturnedSeriesHeader: []string{"0"},
		},
		func(res model.Matrix) error {
			if len(res) != 3 {
				return fmt.Errorf("expected three results. received %d", len(res))
			}

			return nil
		})

	logger.Info("test query returned series limit - above limit disabled")
	requireRangeQuerySuccess(t,
		coordinator,
		resources.RangeQueryRequest{
			Query: "database_write_tagged_success",
			Start: time.Now().Add(-100 * time.Minute),
			End:   time.Now(),
			Step:  15 * time.Second,
		},
		resources.Headers{
			headers.LimitMaxReturnedSeriesHeader: []string{"4"},
		},
		func(res model.Matrix) error {
			if len(res) != 3 {
				return fmt.Errorf("expected three results. received %d", len(res))
			}

			return nil
		})

	logger.Info("test query returned series limit - at limit")
	requireRangeQuerySuccess(t,
		coordinator,
		resources.RangeQueryRequest{
			Query: "database_write_tagged_success",
			Start: time.Now().Add(-100 * time.Minute),
			End:   time.Now(),
			Step:  15 * time.Second,
		},
		resources.Headers{
			headers.LimitMaxReturnedSeriesHeader: []string{"3"},
		},
		func(res model.Matrix) error {
			if len(res) != 3 {
				return fmt.Errorf("expected three results. received %d", len(res))
			}

			return nil
		})

	logger.Info("test query returned series limit - below limit")
	requireRangeQuerySuccess(t,
		coordinator,
		resources.RangeQueryRequest{
			Query: "database_write_tagged_success",
			Start: time.Now().Add(-100 * time.Minute),
			End:   time.Now(),
			Step:  15 * time.Second,
		},
		resources.Headers{
			headers.LimitMaxReturnedSeriesHeader: []string{"2"},
		},
		func(res model.Matrix) error {
			if len(res) != 2 {
				return fmt.Errorf("expected two results. received %d", len(res))
			}

			return nil
		})

	// Test writes to prep for testing returned series metadata limits
	for i := 0; i < 3; i++ {
		err := coordinator.WriteProm("metadata_test_series", map[string]string{
			"metadata_test_label": fmt.Sprintf("series_label_%d", i),
		}, []prompb.Sample{
			{
				Value:     42.42,
				Timestamp: storage.TimeToPromTimestamp(xtime.Now()),
			},
		}, nil)
		require.NoError(t, err)
	}

	logger.Info("test query returned series metadata limit - zero limit disabled")
	requireLabelValuesSuccess(t,
		coordinator,
		resources.LabelValuesRequest{
			MetadataRequest: resources.MetadataRequest{
				Match: "metadata_test_series",
			},
			LabelName: "metadata_test_label",
		},
		resources.Headers{
			headers.LimitMaxReturnedSeriesMetadataHeader: []string{"0"},
		},
		func(res model.LabelValues) error {
			if len(res) != 3 {
				return fmt.Errorf("expected three results. received %d", len(res))
			}

			return nil
		})

	logger.Info("test query returned series metadata limit - above limit disabled")
	requireLabelValuesSuccess(t,
		coordinator,
		resources.LabelValuesRequest{
			MetadataRequest: resources.MetadataRequest{
				Match: "metadata_test_series",
			},
			LabelName: "metadata_test_label",
		},
		resources.Headers{
			headers.LimitMaxReturnedSeriesMetadataHeader: []string{"4"},
		},
		func(res model.LabelValues) error {
			if len(res) != 3 {
				return fmt.Errorf("expected three results. received %d", len(res))
			}

			return nil
		})

	logger.Info("test query returned series metadata limit - at limit")
	requireLabelValuesSuccess(t,
		coordinator,
		resources.LabelValuesRequest{
			MetadataRequest: resources.MetadataRequest{
				Match: "metadata_test_series",
			},
			LabelName: "metadata_test_label",
		},
		resources.Headers{
			headers.LimitMaxReturnedSeriesMetadataHeader: []string{"3"},
		},
		func(res model.LabelValues) error {
			if len(res) != 3 {
				return fmt.Errorf("expected three results. received %d", len(res))
			}

			return nil
		})

	logger.Info("test query returned series metadata limit - below limit")
	requireLabelValuesSuccess(t,
		coordinator,
		resources.LabelValuesRequest{
			MetadataRequest: resources.MetadataRequest{
				Match: "metadata_test_series",
			},
			LabelName: "metadata_test_label",
		},
		resources.Headers{
			headers.LimitMaxReturnedSeriesMetadataHeader: []string{"2"},
		},
		func(res model.LabelValues) error {
			if len(res) != 2 {
				return fmt.Errorf("expected two results. received %d", len(res))
			}

			return nil
		})

	logger.Info("test query time range limit with coordinator defaults")
	requireRangeQuerySuccess(t,
		coordinator,
		resources.RangeQueryRequest{
			Query: "database_write_tagged_success",
			Start: time.Time{},
			End:   time.Now(),
			Step:  15 * time.Second,
		},
		nil,
		func(res model.Matrix) error {
			if len(res) == 0 {
				return errors.New("expected results to be greater than 0")
			}

			return nil
		})

	logger.Info("test query time range limit with require-exhaustive headers false")
	requireRangeQuerySuccess(t,
		coordinator,
		resources.RangeQueryRequest{
			Query: "database_write_tagged_success",
			Start: time.Unix(0, 0),
			End:   time.Now(),
			Step:  15 * time.Second,
		},
		resources.Headers{
			headers.LimitMaxRangeHeader:          []string{"4h"},
			headers.LimitRequireExhaustiveHeader: []string{"false"},
		},
		func(res model.Matrix) error {
			if len(res) == 0 {
				return errors.New("expected results to be greater than 0")
			}

			return nil
		})

	logger.Info("test query time range limit with require-exhaustive headers true " +
		"(above limit therefore error)")
	requireError(t, func() error {
		_, err := coordinator.RangeQuery(resources.RangeQueryRequest{
			Query: "database_write_tagged_success",
			Start: time.Unix(0, 0),
			End:   time.Now(),
			Step:  15 * time.Second,
		}, resources.Headers{
			headers.LimitMaxRangeHeader:          []string{"4h"},
			headers.LimitRequireExhaustiveHeader: []string{"true"},
		})
		return err
	}, "query exceeded limit")
	requireError(t, func() error {
		_, err := coordinator.RangeQuery(resources.RangeQueryRequest{
			Query: "database_write_tagged_success",
			Start: time.Unix(0, 0),
			End:   time.Now(),
			Step:  15 * time.Second,
		}, resources.Headers{
			headers.LimitMaxRangeHeader:          []string{"4h"},
			headers.LimitRequireExhaustiveHeader: []string{"true"},
		})
		return err
	}, "400")

	logger.Info("test query time range limit with coordinator defaults")
	requireLabelValuesSuccess(t,
		coordinator,
		resources.LabelValuesRequest{
			MetadataRequest: resources.MetadataRequest{
				Match: "metadata_test_series",
			},
			LabelName: "metadata_test_label",
		},
		resources.Headers{
			headers.LimitMaxReturnedSeriesMetadataHeader: []string{"2"},
		},
		func(res model.LabelValues) error {
			if len(res) == 0 {
				return errors.New("expected results to be greater than 0")
			}

			return nil
		})

	logger.Info("test query time range limit with require-exhaustive headers false")
	requireLabelValuesSuccess(t,
		coordinator,
		resources.LabelValuesRequest{
			MetadataRequest: resources.MetadataRequest{
				Match: "metadata_test_series",
			},
			LabelName: "metadata_test_label",
		},
		resources.Headers{
			headers.LimitMaxRangeHeader:          []string{"4h"},
			headers.LimitRequireExhaustiveHeader: []string{"false"},
		},
		func(res model.LabelValues) error {
			if len(res) == 0 {
				return errors.New("expected results to be greater than 0")
			}

			return nil
		})

	logger.Info("test query time range limit with require-exhaustive headers true " +
		"(above limit therefore error)")
	requireError(t, func() error {
		_, err := coordinator.LabelValues(resources.LabelValuesRequest{
			MetadataRequest: resources.MetadataRequest{
				Match: "metadata_test_series",
			},
			LabelName: "metadata_test_label",
		}, resources.Headers{
			headers.LimitMaxRangeHeader:          []string{"4h"},
			headers.LimitRequireExhaustiveHeader: []string{"true"},
		})
		return err
	}, "query exceeded limit")
	requireError(t, func() error {
		_, err := coordinator.LabelValues(resources.LabelValuesRequest{
			MetadataRequest: resources.MetadataRequest{
				Match: "metadata_test_series",
			},
			LabelName: "metadata_test_label",
		}, resources.Headers{
			headers.LimitMaxRangeHeader:          []string{"4h"},
			headers.LimitRequireExhaustiveHeader: []string{"true"},
		})
		return err
	}, "400")
}

func testQueryRestrictMetricsType(
	t *testing.T,
	coordinator resources.Coordinator,
	logger *zap.Logger,
) {
	logger.Info("test query restrict to unaggregated metrics type (instant)")
	requireNativeInstantQuerySuccess(t,
		coordinator,
		resources.QueryRequest{
			Query: "bar_metric",
		},
		resources.Headers{
			headers.MetricsTypeHeader: []string{"unaggregated"},
		},
		func(res model.Vector) error {
			if len(res) == 0 {
				return errors.New("expected results. received none")
			}

			if res[0].Value != 42.42 {
				return fmt.Errorf("expected 42.42. received %v", res[0].Value)
			}

			return nil
		})

	logger.Info("test query restrict to unaggregated metrics type (range)")
	requireNativeRangeQuerySuccess(t,
		coordinator,
		resources.RangeQueryRequest{
			Query: "bar_metric",
			Start: time.Now().Add(-1 * time.Hour),
			End:   time.Now(),
			Step:  30 * time.Second,
		},
		resources.Headers{
			headers.MetricsTypeHeader: []string{"unaggregated"},
		},
		func(res model.Matrix) error {
			if len(res) == 0 {
				return errors.New("expected results. received none")
			}

			if len(res[0].Values) == 0 {
				return errors.New("expected values for initial result. received none")
			}

			if res[0].Values[0].Value != 42.42 {
				return fmt.Errorf("expected 42.42. received %v", res[0].Values[0].Value)
			}

			return nil
		})

	logger.Info("test query restrict to aggregated metrics type (instant)")
	requireNativeInstantQuerySuccess(t,
		coordinator,
		resources.QueryRequest{
			Query: "bar_metric",
		},
		resources.Headers{
			headers.MetricsTypeHeader:          []string{"aggregated"},
			headers.MetricsStoragePolicyHeader: []string{"15s:6h"},
		},
		func(res model.Vector) error {
			if len(res) == 0 {
				return errors.New("expected results. received none")
			}

			if res[0].Value != 84.84 {
				return fmt.Errorf("expected 84.84. received %v", res[0].Value)
			}

			return nil
		})

	logger.Info("test query restrict to aggregated metrics type (range)")
	requireNativeRangeQuerySuccess(t,
		coordinator,
		resources.RangeQueryRequest{
			Query: "bar_metric",
			Start: time.Now().Add(-1 * time.Hour),
			End:   time.Now(),
			Step:  30 * time.Second,
		},
		resources.Headers{
			headers.MetricsTypeHeader:          []string{"aggregated"},
			headers.MetricsStoragePolicyHeader: []string{"15s:6h"},
		},
		func(res model.Matrix) error {
			if len(res) == 0 {
				return errors.New("expected results. received none")
			}

			if len(res[0].Values) == 0 {
				return errors.New("expected values for initial result. received none")
			}

			if res[0].Values[0].Value != 84.84 {
				return fmt.Errorf("expected 84.84. received %v", res[0].Values[0].Value)
			}

			return nil
		})
}

func testQueryTimeouts(
	t *testing.T,
	coordinator resources.Coordinator,
	logger *zap.Logger,
) {
	tests := func(timeout, message string) {
		logger.Info(message)
		requireError(t, func() error {
			_, err := coordinator.InstantQuery(resources.QueryRequest{
				Query: "database_write_tagged_success",
			}, resources.Headers{
				headers.TimeoutHeader: []string{timeout},
			})
			return err
		}, "504")

		requireError(t, func() error {
			_, err := coordinator.RangeQuery(resources.RangeQueryRequest{
				Query: "database_write_tagged_success",
				Start: time.Unix(0, 0),
				End:   time.Now(),
			}, resources.Headers{
				headers.TimeoutHeader: []string{timeout},
			})
			return err
		}, "504")

		requireError(t, func() error {
			_, err := coordinator.LabelNames(resources.LabelNamesRequest{},
				resources.Headers{
					headers.TimeoutHeader: []string{timeout},
				})
			return err
		}, "504")

		requireError(t, func() error {
			_, err := coordinator.LabelValues(resources.LabelValuesRequest{
				LabelName: "__name__",
			}, resources.Headers{
				headers.TimeoutHeader: []string{timeout},
			})
			return err
		}, "504")
	}

	tests("1ns", "test timeouts at the coordinator layer")
	tests("1ms", "test timeouts at the coordinator -> m3db layer")
}

func testPrometheusQueryNativeTimeout(
	t *testing.T,
	coordinator resources.Coordinator,
	logger *zap.Logger,
) {
	logger.Info("test query gateway timeout (instant)")
	requireError(t, func() error {
		_, err := coordinator.InstantQueryWithEngine(resources.QueryRequest{
			Query: "bar_metric",
		}, options.M3QueryEngine, resources.Headers{
			headers.TimeoutHeader:     []string{"1ms"},
			headers.MetricsTypeHeader: []string{"unaggregated"},
		})
		return err
	}, "504")

	logger.Info("test query gateway timeout (range)")
	requireError(t, func() error {
		_, err := coordinator.RangeQueryWithEngine(resources.RangeQueryRequest{
			Query: "bar_metric",
			Start: time.Now().Add(-1 * time.Hour),
			End:   time.Now(),
			Step:  30 * time.Second,
		}, options.M3QueryEngine, resources.Headers{
			headers.TimeoutHeader:     []string{"1ms"},
			headers.MetricsTypeHeader: []string{"unaggregated"},
		})
		return err
	}, "504")
}

func testQueryRestrictTags(
	t *testing.T,
	coordinator resources.Coordinator,
	logger *zap.Logger,
) {
	// Test the default restrict tags is applied when directly querying
	// coordinator (restrict tags set to hide any restricted_metrics_type="hidden"
	// in m3coordinator.yml)

	// First write some hidden metrics.
	logger.Info("test write with unaggregated metrics type works as expected")
	require.NoError(t, coordinator.WriteProm("some_hidden_metric", map[string]string{
		"restricted_metrics_type": "hidden",
		"foo_tag":                 "foo_tag_value",
	}, []prompb.Sample{
		{
			Value:     42.42,
			Timestamp: storage.TimeToPromTimestamp(xtime.Now()),
		},
	}, nil))

	// Check that we can see them with zero restrictions applied as an
	// override (we do this check first so that when we test that they
	// don't appear by default we know that the metrics are already visible).
	logger.Info("test restrict by tags with header override to remove restrict works")
	requireInstantQuerySuccess(t, coordinator, resources.QueryRequest{
		Query: "{restricted_metrics_type=\"hidden\"}",
	}, resources.Headers{
		headers.RestrictByTagsJSONHeader: []string{"{}"},
	}, func(res model.Vector) error {
		if len(res) != 1 {
			return fmt.Errorf("expected 1 result, got %v", len(res))
		}
		return nil
	})

	// Now test that the defaults will hide the metrics altogether.
	logger.Info("test restrict by tags with coordinator defaults")
	requireInstantQuerySuccess(t, coordinator, resources.QueryRequest{
		Query: "{restricted_metrics_type=\"hidden\"}",
	}, nil, func(res model.Vector) error {
		if len(res) != 0 {
			return fmt.Errorf("expected no results, got %v", len(res))
		}
		return nil
	})
}

func testPrometheusRemoteWriteMapTags(
	t *testing.T,
	coordinator resources.Coordinator,
	logger *zap.Logger,
) {
	logger.Info("test map tags header works as expected")
	require.NoError(t, coordinator.WriteProm("bar_metric", nil, []prompb.Sample{
		{
			Value:     42.42,
			Timestamp: storage.TimeToPromTimestamp(xtime.Now()),
		},
	}, resources.Headers{
		headers.MetricsTypeHeader:   []string{"unaggregated"},
		headers.MapTagsByJSONHeader: []string{`{"tagMappers":[{"write":{"tag":"globaltag","value":"somevalue"}}]}`},
	}))

	requireNativeInstantQuerySuccess(t, coordinator, resources.QueryRequest{
		Query: "bar_metric",
	}, resources.Headers{
		headers.MetricsTypeHeader: []string{"unaggregated"},
	}, func(res model.Vector) error {
		if len(res) == 0 {
			return errors.New("expecting results, got none")
		}

		if val, ok := res[0].Metric["globaltag"]; !ok || val != "somevalue" {
			return fmt.Errorf("expected metric with globaltag=somevalue, got=%+v", res[0].Metric)
		}

		return nil
	})
}

func testSeries(t *testing.T, coordinator resources.Coordinator, logger *zap.Logger) {
	logger.Info("test series match endpoint")
	requireSeriesSuccess(t, coordinator, resources.SeriesRequest{
		MetadataRequest: resources.MetadataRequest{
			Match: "prometheus_remote_storage_samples_total",
			Start: time.Unix(0, 0),
			End:   time.Now().Add(1 * time.Hour),
		},
	}, nil, func(res []model.Metric) error {
		if len(res) != 1 {
			return fmt.Errorf("expected 1 result, got %v", len(res))
		}
		return nil
	})

	requireSeriesSuccess(t, coordinator, resources.SeriesRequest{
		MetadataRequest: resources.MetadataRequest{
			Match: "prometheus_remote_storage_samples_total",
		},
	}, nil, func(res []model.Metric) error {
		if len(res) != 1 {
			return fmt.Errorf("expected 1 result, got %v", len(res))
		}
		return nil
	})

	// NB(nate): Use raw RunQuery method here since we want to use a custom format for start
	// and end
	queryAndParms := "api/v1/series?match[]=prometheus_remote_storage_samples_total&start=" +
		"-292273086-05-16T16:47:06Z&end=292277025-08-18T07:12:54.999999999Z"
	require.NoError(t, coordinator.RunQuery(
		func(status int, headers map[string][]string, resp string, err error) error {
			if status != http.StatusOK {
				return fmt.Errorf("expected 200, got %d. body=%v", status, resp)
			}
			var parsedResp seriesResponse
			if err := json.Unmarshal([]byte(resp), &parsedResp); err != nil {
				return err
			}

			if len(parsedResp.Data) != 1 {
				return fmt.Errorf("expected 1 result, got %d", len(parsedResp.Data))
			}

			return nil
		}, queryAndParms, nil))
}

func testLabelQueryLimitsApplied(
	t *testing.T,
	coordinator resources.Coordinator,
	logger *zap.Logger,
) {
	logger.Info("test label limits with require-exhaustive headers true " +
		"(below limit therefore no error)")
	requireLabelValuesSuccess(t,
		coordinator,
		resources.LabelValuesRequest{
			LabelName: "__name__",
		},
		resources.Headers{
			headers.LimitMaxSeriesHeader:         []string{"10000"},
			headers.LimitRequireExhaustiveHeader: []string{"true"},
		},
		func(res model.LabelValues) error {
			// NB(nate): just checking for a 200 and this method only gets called in that case
			return nil
		})

	logger.Info("test label series limit with coordinator limit header (default " +
		"requires exhaustive so error)")
	requireError(t, func() error {
		_, err := coordinator.LabelValues(resources.LabelValuesRequest{
			LabelName: "__name__",
		}, resources.Headers{
			headers.LimitMaxSeriesHeader: []string{"1"},
		})
		return err
	}, "query exceeded limit")

	logger.Info("test label series limit with require-exhaustive headers false")
	requireLabelValuesSuccess(t,
		coordinator,
		resources.LabelValuesRequest{
			LabelName: "__name__",
		},
		resources.Headers{
			headers.LimitMaxSeriesHeader:         []string{"2"},
			headers.LimitRequireExhaustiveHeader: []string{"false"},
		},
		func(res model.LabelValues) error {
			if len(res) != 1 {
				return fmt.Errorf("expected 1 result, got %d", len(res))
			}
			return nil
		})

	logger.Info("Test label series limit with require-exhaustive headers " +
		"true (above limit therefore error)")
	requireError(t, func() error {
		_, err := coordinator.LabelValues(resources.LabelValuesRequest{
			LabelName: "__name__",
		}, resources.Headers{
			headers.LimitMaxSeriesHeader:         []string{"2"},
			headers.LimitRequireExhaustiveHeader: []string{"true"},
		})
		return err
	}, "query exceeded limit")
	requireError(t, func() error {
		_, err := coordinator.LabelValues(resources.LabelValuesRequest{
			LabelName: "__name__",
		}, resources.Headers{
			headers.LimitMaxSeriesHeader:         []string{"2"},
			headers.LimitRequireExhaustiveHeader: []string{"true"},
		})
		return err
	}, "400")

	logger.Info("test label docs limit with coordinator limit header " +
		"(default requires exhaustive so error)")
	requireError(t, func() error {
		_, err := coordinator.LabelValues(resources.LabelValuesRequest{
			LabelName: "__name__",
		}, resources.Headers{
			headers.LimitMaxDocsHeader: []string{"1"},
		})
		return err
	}, "query exceeded limit")

	logger.Info("test label docs limit with require-exhaustive headers false")
	requireLabelValuesSuccess(t,
		coordinator,
		resources.LabelValuesRequest{
			LabelName: "__name__",
		},
		resources.Headers{
			headers.LimitMaxDocsHeader:           []string{"2"},
			headers.LimitRequireExhaustiveHeader: []string{"false"},
		},
		func(res model.LabelValues) error {
			if len(res) != 1 {
				return fmt.Errorf("expected 1 result, got %d", len(res))
			}
			return nil
		})

	logger.Info("Test label docs limit with require-exhaustive headers " +
		"true (above limit therefore error)")
	requireError(t, func() error {
		_, err := coordinator.LabelValues(resources.LabelValuesRequest{
			LabelName: "__name__",
		}, resources.Headers{
			headers.LimitMaxSeriesHeader:         []string{"1"},
			headers.LimitRequireExhaustiveHeader: []string{"true"},
		})
		return err
	}, "query exceeded limit")
	requireError(t, func() error {
		_, err := coordinator.LabelValues(resources.LabelValuesRequest{
			LabelName: "__name__",
		}, resources.Headers{
			headers.LimitMaxSeriesHeader:         []string{"1"},
			headers.LimitRequireExhaustiveHeader: []string{"true"},
		})
		return err
	}, "400")
}

func testLabels(
	t *testing.T,
	coordinator resources.Coordinator,
	logger *zap.Logger,
) {
	logger.Info("test label APIs")
	require.NoError(t, coordinator.WriteProm("label_metric", map[string]string{
		"name_0": "value_0_1",
		"name_1": "value_1_1",
		"name_2": "value_2_1",
	}, []prompb.Sample{
		{
			Value:     42.42,
			Timestamp: storage.TimeToPromTimestamp(xtime.Now()),
		},
	}, nil))

	require.NoError(t, coordinator.WriteProm("label_metric_2", map[string]string{
		"name_0": "value_0_2",
		"name_1": "value_1_2",
	}, []prompb.Sample{
		{
			Value:     42.42,
			Timestamp: storage.TimeToPromTimestamp(xtime.Now()),
		},
	}, nil))

	requireLabelNamesSuccess(t, coordinator, resources.LabelNamesRequest{}, nil,
		func(res model.LabelNames) error {
			var nameLabels model.LabelNames
			for _, label := range res {
				matched, err := regexp.MatchString("name_[012]", string(label))
				if err != nil {
					return err
				}
				if matched {
					nameLabels = append(nameLabels, label)
				}
			}
			if len(nameLabels) != 3 {
				return fmt.Errorf("expected 3 results, got %d", len(nameLabels))
			}
			return nil
		})

	requireLabelNamesSuccess(t, coordinator, resources.LabelNamesRequest{
		MetadataRequest: resources.MetadataRequest{
			Match: "label_metric",
		},
	}, nil, func(res model.LabelNames) error {
		if len(res) != 4 {
			return fmt.Errorf("expected 4 results, got %d", len(res))
		}
		return nil
	})

	requireLabelNamesSuccess(t, coordinator, resources.LabelNamesRequest{
		MetadataRequest: resources.MetadataRequest{
			Match: "label_metric_2",
		},
	}, nil, func(res model.LabelNames) error {
		if len(res) != 3 {
			return fmt.Errorf("expected 3 results, got %d", len(res))
		}
		return nil
	})

	requireLabelValuesSuccess(t, coordinator, resources.LabelValuesRequest{
		LabelName: "name_1",
	}, nil, func(res model.LabelValues) error {
		if len(res) != 2 {
			return fmt.Errorf("expected 2 results, got %d", len(res))
		}
		return nil
	})

	tests := func(match string, length int, val string) {
		requireLabelValuesSuccess(t, coordinator, resources.LabelValuesRequest{
			MetadataRequest: resources.MetadataRequest{
				Match: match,
			},
			LabelName: "name_1",
		}, nil, func(res model.LabelValues) error {
			if len(res) != length {
				return fmt.Errorf("expected %d results, got %d", length, len(res))
			}
			return nil
		})

		requireLabelValuesSuccess(t, coordinator, resources.LabelValuesRequest{
			MetadataRequest: resources.MetadataRequest{
				Match: match,
			},
			LabelName: "name_1",
		}, nil, func(res model.LabelValues) error {
			if string(res[0]) != val {
				return fmt.Errorf("expected %s, got %s", val, res[0])
			}
			return nil
		})
	}
	tests("label_metric", 1, "value_1_1")
	tests("label_metric_2", 1, "value_1_2")
}

type seriesResponse struct {
	Status string
	Data   []map[string]string
}

func requireError(t *testing.T, query func() error, errorMsg string) {
	require.NoError(t, resources.Retry(func() error {
		if err := query(); err != nil {
			if errorMsg == "" || strings.Contains(err.Error(), errorMsg) {
				return nil
			}
		}

		err := errors.New("expected read request to fail with error")
		if errorMsg == "" {
			err = fmt.Errorf("expected read request to fail with error containing: %s", errorMsg)
		}

		return err
	}))
}

func requireInstantQuerySuccess(
	t *testing.T,
	coordinator resources.Coordinator,
	request resources.QueryRequest,
	headers resources.Headers,
	successCond func(res model.Vector) error,
) {
	require.NoError(t, resources.Retry(func() error {
		res, err := coordinator.InstantQuery(request, headers)
		if err != nil {
			return err
		}

		return successCond(res)
	}))
}

func requireNativeInstantQuerySuccess(
	t *testing.T,
	coordinator resources.Coordinator,
	request resources.QueryRequest,
	headers resources.Headers,
	successCond func(res model.Vector) error,
) {
	require.NoError(t, resources.Retry(func() error {
		res, err := coordinator.InstantQueryWithEngine(request, options.M3QueryEngine, headers)
		if err != nil {
			return err
		}

		return successCond(res)
	}))
}

func requireRangeQuerySuccess(
	t *testing.T,
	coordinator resources.Coordinator,
	request resources.RangeQueryRequest,
	headers resources.Headers,
	successCond func(res model.Matrix) error,
) {
	require.NoError(t, resources.Retry(func() error {
		res, err := coordinator.RangeQuery(request, headers)
		if err != nil {
			return err
		}

		return successCond(res)
	}))
}

func requireNativeRangeQuerySuccess(
	t *testing.T,
	coordinator resources.Coordinator,
	request resources.RangeQueryRequest,
	headers resources.Headers,
	successCond func(res model.Matrix) error,
) {
	require.NoError(t, resources.Retry(func() error {
		res, err := coordinator.RangeQueryWithEngine(request, options.M3QueryEngine, headers)
		if err != nil {
			return err
		}

		return successCond(res)
	}))
}

func requireLabelValuesSuccess(
	t *testing.T,
	coordinator resources.Coordinator,
	request resources.LabelValuesRequest,
	headers resources.Headers,
	successCond func(res model.LabelValues) error,
) {
	require.NoError(t, resources.Retry(func() error {
		res, err := coordinator.LabelValues(request, headers)
		if err != nil {
			return err
		}

		return successCond(res)
	}))
}

func requireLabelNamesSuccess(
	t *testing.T,
	coordinator resources.Coordinator,
	request resources.LabelNamesRequest,
	headers resources.Headers,
	successCond func(res model.LabelNames) error,
) {
	require.NoError(t, resources.Retry(func() error {
		res, err := coordinator.LabelNames(request, headers)
		if err != nil {
			return err
		}

		return successCond(res)
	}))
}

func requireSeriesSuccess(
	t *testing.T,
	coordinator resources.Coordinator,
	request resources.SeriesRequest,
	headers resources.Headers,
	successCond func(res []model.Metric) error,
) {
	require.NoError(t, resources.Retry(func() error {
		res, err := coordinator.Series(request, headers)
		if err != nil {
			return err
		}

		return successCond(res)
	}))
}

func verifyPrometheusQuery(t *testing.T, p *docker.Prometheus, query string, threshold float64) {
	require.NoError(t, resources.Retry(func() error {
		res, err := p.Query(docker.PrometheusQueryRequest{
			Query: query,
		})
		if err != nil {
			return err
		}
		if len(res) == 0 {
			return errors.New("no samples returned for query")
		}
		if res[0].Value > model.SampleValue(threshold) {
			return nil
		}

		return errors.New("value not greater than threshold")
	}))
}
