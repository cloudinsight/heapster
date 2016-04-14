// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudinsight

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"k8s.io/heapster/metrics/core"

	"github.com/golang/glog"
)

type CloudInsightConfig struct {
	Endpoint string
	Token    string
	Host     string
}

type cloudinsightSink struct {
	config           *CloudInsightConfig
	numberOfFailures int
	batchSize        int
	sync.RWMutex
}
type JsonPostMetric struct {
	Host    string        `json:"host"`
	Metrics []interface{} `json:"metrics"`
}

type CiDataPoint struct {
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags"`
	Value     float64           `json:"value"`
	Timestamp int64             `json:"timestamp"`
}

func BuildConfig(uri *url.URL) (*CloudInsightConfig, error) {
	config := CloudInsightConfig{
		Token:    "",
		Host:     "",
		Endpoint: "http://localhost:8080/api/v1/metrics",
	}

	if len(uri.Host) > 0 && len(uri.Path) > 0 {
		// TODO scheme
		config.Endpoint = "http://" + uri.Host + "/" + uri.Path
	}

	opts := uri.Query()

	glog.Warningf("uri: %#v", uri)
	glog.Warningf("opts: %#v", opts)

	if len(opts["token"]) >= 1 {
		config.Token = opts["token"][0]
	} else {
		return nil, errors.New("Token is not valid.")
	}

	if len(opts["host"]) >= 1 {
		config.Host = opts["host"][0]
	} else {
		return nil, errors.New("Host is not valid.")
	}

	return &config, nil
}

// Call Cloud Insight HTTP API
func (sink *cloudinsightSink) SendRequest(request []CiDataPoint) (int, string, error) {

	// marshal
	var rawRequest io.Reader

	var metrics [](interface{})
	var m2 [](interface{})
	metrics = make([](interface{}), 0, 0)

	for _, m1 := range request {
		m2 = make([](interface{}), 0, 0)
		m2 = append(m2, m1.Metric)
		m2 = append(m2, m1.Timestamp)
		m2 = append(m2, m1.Value)
		m2 = append(m2, m1.Tags)
		metrics = append(metrics, m2)
	}

	if request != nil {
		jpm := JsonPostMetric{
			Host:    sink.config.Host,
			Metrics: metrics,
		}
		jsonRequest, err := json.Marshal(jpm)
		if err != nil {
			return 0, "", err
		}
		rawRequest = bytes.NewReader(jsonRequest)
		fmt.Println("jsonRequest :", string(jsonRequest))
	}

	req, err := http.NewRequest("POST", sink.config.Endpoint, rawRequest)
	if err != nil {
		return 0, "", err
	}

	req.Header.Set("X-Token", sink.config.Token)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		return 0, "", err
	}

	defer resp.Body.Close()

	fmt.Println("response Status:", resp.Status)
	fmt.Println("response StatusCode:", resp.StatusCode)
	fmt.Println("response Headers:", resp.Header)

	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return 0, "", err
	}

	fmt.Println("response Body:", string(body))

	return resp.StatusCode, string(body), nil
}

// Pushes the specified metric measurement to the CloudinSight API.
// The Timeseries are transformed to CloudinSight metrics beforehand.
// Timeseries that cannot be translated to CloudinSight metrics are skipped.
func (sink *cloudinsightSink) ExportData(dataBatch *core.DataBatch) {
	sink.Lock()
	defer sink.Unlock()

	metrics := sink.processDataBatch(dataBatch)
	code, response, err := sink.SendRequest(metrics)
	if err != nil {
		glog.Errorf("%s", err)
		sink.numberOfFailures++
		return
	}
	if code != http.StatusNoContent {
		glog.Error(response)
		sink.numberOfFailures++
	}
}

func (sink *cloudinsightSink) processDataBatch(dataBatch *core.DataBatch) []CiDataPoint {
	metrics := []CiDataPoint{}
	for _, metricSet := range dataBatch.MetricSets {
		m := sink.processMetricSet(dataBatch, metricSet)
		metrics = append(metrics, m...)
	}
	return metrics
}

func (sink *cloudinsightSink) processMetricSet(dataBatch *core.DataBatch, metricSet *core.MetricSet) []CiDataPoint {
	metrics := []CiDataPoint{}

	// process unlabeled metrics
	for metricName, metricValue := range metricSet.MetricValues {
		m := sink.processMetric(metricSet.Labels, metricName, dataBatch.Timestamp, metricValue.GetValue())
		if nil != m {
			metrics = append(metrics, *m)
		}
	}

	// process labeled metrics
	for _, metric := range metricSet.LabeledMetrics {
		labels := map[string]string{}
		for k, v := range metricSet.Labels {
			labels[k] = v
		}
		for k, v := range metric.Labels {
			labels[k] = v
		}
		m := sink.processMetric(labels, metric.Name, dataBatch.Timestamp, metric.GetValue())
		if nil != m {
			metrics = append(metrics, *m)
		}
	}
	return metrics
}

func (sink *cloudinsightSink) processMetric(labels map[string]string, name string, timestamp time.Time, value interface{}) *CiDataPoint {
	val, err := sink.convertValue(value)
	if err != nil {
		glog.Warningf("Metric cannot be pushed to CloudinSight. %#v", value)
		return nil
	}
	dims := sink.processLabels(labels)

	// TOOD delete type key from dims
	metric_type := dims["type"]

	m := CiDataPoint{
		Metric:    "kubernetes." + metric_type + "." + strings.Replace(name, "/", ".", -1),
		Tags:      dims,
		Timestamp: (timestamp.UnixNano() / 1000000000),
		Value:     val,
	}
	return &m
}

// convert the Timeseries value to a CloudinSight value
func (sink *cloudinsightSink) convertValue(val interface{}) (float64, error) {
	switch val.(type) {
	case int:
		return float64(val.(int)), nil
	case int64:
		return float64(val.(int64)), nil
	case bool:
		if val.(bool) {
			return 1.0, nil
		}
		return 0.0, nil
	case float32:
		return float64(val.(float32)), nil
	case float64:
		return val.(float64), nil
	}
	return 0.0, fmt.Errorf("Unsupported CloudinSight metric value type %T", reflect.TypeOf(val))
}

// preprocesses heapster labels, splitting into CloudinSight dimensions and CloudinSight meta-values
func (sink *cloudinsightSink) processLabels(labels map[string]string) map[string]string {
	dims := map[string]string{}

	// labels to dimensions

	dims["host"] = sink.processDimension(labels[core.LabelHostname.Key])
	// dims[core.LabelContainerName.Key] = sink.processDimension(labels[core.LabelContainerName.Key])
	dims["service"] = "kubernetes"

	// other labels
	for i, v := range labels {
		if i != core.LabelPodName.Key && i != core.LabelHostname.Key &&
			i != core.LabelContainerName.Key && v != "" {
			dims[i] = strings.Replace(v, ",", " ", -1)
		}
	}
	return dims
}

// creates a valid dimension value
func (sink *cloudinsightSink) processDimension(value string) string {
	if value != "" {
		v := strings.Replace(value, "/", ".", -1)
		return strings.Replace(v, ",", " ", -1)
	}
	return "none"
}

func (sink *cloudinsightSink) ExportData2(dataBatch *core.DataBatch) {
	sink.Lock()
	defer sink.Unlock()

	dataPoints := make([]CiDataPoint, 0, 0)
	for _, metricSet := range dataBatch.MetricSets {
		for metricName, metricValue := range metricSet.MetricValues {

			var value interface{}
			if core.ValueInt64 == metricValue.ValueType {
				value = metricValue.IntValue
			} else if core.ValueFloat == metricValue.ValueType {
				value = float64(metricValue.FloatValue)
			} else {
				continue
			}

			point := CiDataPoint{
				Metric:    metricName,
				Tags:      metricSet.Labels,
				Value:     value.(float64),
				Timestamp: (dataBatch.Timestamp.UnixNano() / 1000000), //dataBatch.Timestamp.UTC(),
			}
			dataPoints = append(dataPoints, point)

		}

		for _, labeledMetric := range metricSet.LabeledMetrics {

			var value interface{}
			if core.ValueInt64 == labeledMetric.ValueType {
				value = labeledMetric.IntValue
			} else if core.ValueFloat == labeledMetric.ValueType {
				value = float64(labeledMetric.FloatValue)
			} else {
				continue
			}

			point := CiDataPoint{
				Metric:    labeledMetric.Name,
				Tags:      make(map[string]string),
				Value:     value.(float64),
				Timestamp: (dataBatch.Timestamp.UnixNano() / 1000000), //dataBatch.Timestamp.UTC(),
			}
			for key, value := range metricSet.Labels {
				point.Tags[key] = value
			}
			for key, value := range labeledMetric.Labels {
				point.Tags[key] = value
			}

			dataPoints = append(dataPoints, point)

		}
	}

	if len(dataPoints) >= 0 {
		sink.sendData(dataPoints)
	}
}

func (sink *cloudinsightSink) sendData(dataPoints []CiDataPoint) {

	start := time.Now()

	end := time.Now()
	glog.V(4).Infof("Exported %d data to CloudInsight in %s", len(dataPoints), end.Sub(start))
}

func (sink *cloudinsightSink) Name() string {
	return "Cloud Insight Sink"
}

func (sink *cloudinsightSink) Stop() {
	// nothing needs to be done.
}

func CreateCloudinsightSink(uri *url.URL) (core.DataSink, error) {
	c, err := BuildConfig(uri)
	if err != nil {
		return nil, err
	}
	sink := cloudinsightSink{
		config:           c,
		numberOfFailures: 1000,
		batchSize:        100,
	}

	glog.Infof("created cloudinsight sink with options: endpoint: %s host:%s token:%s", c.Endpoint, c.Host, c.Token)
	return &sink, nil
}
