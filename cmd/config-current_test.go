/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"os"
	"path"
	"testing"
)

func TestServerConfig(t *testing.T) {
	objLayer, fsDir, err := prepareFS()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(fsDir)

	if err = newTestConfig(globalMinioDefaultRegion, objLayer); err != nil {
		t.Fatalf("Init Test config failed")
	}

	if globalServerConfig.GetRegion() != globalMinioDefaultRegion {
		t.Errorf("Expecting region `us-east-1` found %s", globalServerConfig.GetRegion())
	}

	// Set new region and verify.
	globalServerConfig.SetRegion("us-west-1")
	if globalServerConfig.GetRegion() != "us-west-1" {
		t.Errorf("Expecting region `us-west-1` found %s", globalServerConfig.GetRegion())
	}

	// Match version.
	if globalServerConfig.GetVersion() != serverConfigVersion {
		t.Errorf("Expecting version %s found %s", globalServerConfig.GetVersion(), serverConfigVersion)
	}

	if err := saveServerConfig(context.Background(), objLayer, globalServerConfig); err != nil {
		t.Fatalf("Unable to save updated config file %s", err)
	}

	// Initialize server config.
	if err := loadConfig(objLayer); err != nil {
		t.Fatalf("Unable to initialize from updated config file %s", err)
	}
}

// Tests config validator..
func TestValidateConfig(t *testing.T) {
	objLayer, fsDir, err := prepareFS()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(fsDir)

	if err = newTestConfig(globalMinioDefaultRegion, objLayer); err != nil {
		t.Fatalf("Init Test config failed")
	}

	configPath := path.Join(minioConfigPrefix, minioConfigFile)
	v := serverConfigVersion

	testCases := []struct {
		configData string
		shouldPass bool
	}{
		// Test 1 - wrong json
		{`{`, false},

		// Test 2 - empty json
		{`{}`, false},

		// Test 3 - wrong config version
		{`{"version": "10"}`, false},

		// Test 4 - wrong browser parameter
		{`{"version": "` + v + `", "browser": "foo"}`, false},

		// Test 5 - missing credential
		{`{"version": "` + v + `", "browser": "on"}`, false},

		// Test 6 - missing secret key
		{`{"version": "` + v + `", "browser": "on", "credential" : {"accessKey":"minio", "secretKey":""}}`, false},

		// Test 7 - missing region should pass, defaults to 'us-east-1'.
		{`{"version": "` + v + `", "browser": "on", "credential" : {"accessKey":"minio", "secretKey":"minio123"}}`, true},

		// Test 8 - missing browser should pass, defaults to 'on'.
		{`{"version": "` + v + `", "region": "us-east-1", "credential" : {"accessKey":"minio", "secretKey":"minio123"}}`, true},

		// Test 9 - success
		{`{"version": "` + v + `", "browser": "on", "region":"us-east-1", "credential" : {"accessKey":"minio", "secretKey":"minio123"}}`, true},

		// Test 10 - duplicated json keys
		{`{"version": "` + v + `", "browser": "on", "browser": "on", "region":"us-east-1", "credential" : {"accessKey":"minio", "secretKey":"minio123"}}`, false},

		// Test 11 - Test AMQP
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "amqp": { "1": { "enable": true, "url": "", "exchange": "", "routingKey": "", "exchangeType": "", "mandatory": false, "immediate": false, "durable": false, "internal": false, "noWait": false, "autoDeleted": false, "queueDir": "", "queueLimit": 0}}}}`, false},

		// Test 12 - Test NATS
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "nats": { "1": { "enable": true, "address": "", "subject": "", "username": "", "password": "", "token": "", "secure": false, "pingInterval": 0, "queueDir": "", "queueLimit": 0, "streaming": { "enable": false, "clusterID": "", "async": false, "maxPubAcksInflight": 0 } } }}}`, false},

		// Test 13 - Test ElasticSearch
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "elasticsearch": { "1": { "enable": true, "url": "", "index": "" } }}}`, false},

		// Test 14 - Test Redis
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "redis": { "1": { "enable": true, "address": "", "password": "", "key": "" } }}}`, false},

		// Test 15 - Test PostgreSQL
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "postgresql": { "1": { "enable": true, "connectionString": "", "table": "", "host": "", "port": "", "user": "", "password": "", "database": "", "queueDir": "", "queueLimit": 0 }}}}`, false},

		// Test 16 - Test Kafka
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "kafka": { "1": { "enable": true, "brokers": null, "topic": "", "queueDir": "", "queueLimit": 0 } }}}`, false},

		// Test 17 - Test Webhook
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "webhook": { "1": { "enable": true, "endpoint": "", "queueDir": "", "queueLimit": 0} }}}`, false},

		// Test 18 - Test MySQL
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "mysql": { "1": { "enable": true, "dsnString": "",  "table": "", "host": "", "port": "", "user": "", "password": "", "database": "", "queueDir": "", "queueLimit": 0 }}}}`, false},

		// Test 19 - Test Format for MySQL
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "mysql": { "1": { "enable": true, "dsnString": "",  "format": "invalid", "table": "xxx", "host": "10.0.0.1", "port": "3306", "user": "abc", "password": "pqr", "database": "test1" }}}}`, false},

		// Test 20 - Test valid Format for MySQL
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "mysql": { "1": { "enable": true, "dsnString": "",  "format": "namespace", "table": "xxx", "host": "10.0.0.1", "port": "3306", "user": "abc", "password": "pqr", "database": "test1" }}}}`, true},

		// Test 21 - Test Format for PostgreSQL
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "postgresql": { "1": { "enable": true, "connectionString": "", "format": "invalid", "table": "xxx", "host": "myhost", "port": "5432", "user": "abc", "password": "pqr", "database": "test1" }}}}`, false},

		// Test 22 - Test valid Format for PostgreSQL
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "postgresql": { "1": { "enable": true, "connectionString": "", "format": "namespace", "table": "xxx", "host": "myhost", "port": "5432", "user": "abc", "password": "pqr", "database": "test1" }}}}`, true},

		// Test 23 - Test Format for ElasticSearch
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "elasticsearch": { "1": { "enable": true, "format": "invalid", "url": "example.com", "index": "myindex" } }}}`, false},

		// Test 24 - Test valid Format for ElasticSearch
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "elasticsearch": { "1": { "enable": true, "format": "namespace", "url": "example.com", "index": "myindex", "queueDir": "", "queueLimit": 0 } }}}`, true},

		// Test 25 - Test Format for Redis
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "redis": { "1": { "enable": true, "format": "invalid", "address": "example.com:80", "password": "xxx", "key": "key1", "queueDir": "", "queueLimit": 0 } }}}`, false},

		// Test 26 - Test valid Format for Redis
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "redis": { "1": { "enable": true, "format": "namespace", "address": "example.com:80", "password": "xxx", "key": "key1" } }}}`, true},

		// Test 27 - Test MQTT
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "mqtt": { "1": { "enable": true, "broker": "",  "topic": "", "qos": 0, "username": "", "password": "", "queueDir": "", "queueLimit": 0}}}}`, false},

		// Test 28 - Test NSQ
		{`{"version": "` + v + `", "credential": { "accessKey": "minio", "secretKey": "minio123" }, "region": "us-east-1", "browser": "on", "notify": { "nsq": { "1": { "enable": true, "nsqdAddress": "", "topic": "", "queueDir": "", "queueLimit": 0} }}}`, false},
	}

	for i, testCase := range testCases {
		if err = saveConfig(context.Background(), objLayer, configPath, []byte(testCase.configData)); err != nil {
			t.Fatal(err)
		}
		_, err = getValidConfig(objLayer)
		if testCase.shouldPass && err != nil {
			t.Errorf("Test %d, should pass but it failed with err = %v", i+1, err)
		}
		if !testCase.shouldPass && err == nil {
			t.Errorf("Test %d, should fail but it succeeded.", i+1)
		}
	}

}
