

/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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

 package main

 import (
	 "fmt"
	 "log"
	 "net/http"
	 "io/ioutil"
	 "encoding/json"
	 "github.com/minio/minio/pkg/event"
	 "github.com/minio/minio/pkg/event/target"
 )

 var mqttArgs = []byte(`{
	"enable": true,
	"broker": "tcp://localhost:1884",
	"topic": "minio",
	"qos": 1,
	"clientId": "",
	"username": "",
	"password": "",
	"reconnectInterval": 0,
	"keepAliveInterval": 0
  }`)

 
func main() {

	var id int
	var err error
	var args target.MQTTArgs

	err = json.Unmarshal(mqttArgs, &args)
	if err != nil {
		fmt.Println("error: ", err.Error())
		return
	}

	err = args.Validate()
	if err != nil {
		fmt.Println("error: ", err.Error())
		return
	}

	newTarget, err := target.NewMQTTTarget(string(id), args)
	id = id + 1
	if err != nil {
		fmt.Println("error: ", err.Error())
		return
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		b, err := ioutil.ReadAll(r.Body)
		defer r.Body.Close()
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		fmt.Println("got")
		//fmt.Println(string(b))
		var msg event.Log
		err = json.Unmarshal(b, &msg)
		fmt.Println("unmarshalled json")
		fmt.Println(msg.Records)
		err = newTarget.SendFromWebhook(args.Topic, args.QoS, msg)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		// Unmarshal
		// var msg Message
		// err = json.Unmarshal(b, &msg)
		

		// output, err := json.Marshal(msg)
		// if err != nil {
		// 	http.Error(w, err.Error(), 500)
		// 	return
		// }
		// w.Header().Set("content-type", "application/json")
		w.Write([]byte("got response")) 
	})

	log.Printf("listening on http://%s/", "localhost:8080")
	log.Fatal(http.ListenAndServe("localhost:8080", nil))
 }
 