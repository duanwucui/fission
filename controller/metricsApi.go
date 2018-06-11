/*
Copyright 2016 The Fission Authors.

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

package controller

import (
	"encoding/json"
	"net/http"
)

func (a *API) TotalRequestsToFunc(w http.ResponseWriter, r *http.Request) {
	//vars := mux.Vars(r)
	//name := vars["configmap"]
	url := a.extractQueryParamFromRequest(r, "url")
	method := a.extractQueryParamFromRequest(r, "method")
	timeDurationStr := a.extractQueryParamFromRequest(r, "window")
	fn := a.extractQueryParamFromRequest(r, "function")
	fns := a.extractQueryParamFromRequest(r, "namespace")
	//timeDuration, err := time.ParseDuration(timeDurationStr)
	//if err != nil {
	//	log.Printf("Error parsing time duration :%v", err)
	//	a.respondWithError(w, err)
	//	return
	//}

	result, err := a.promClient.GetTotalRequestToFunc(url, method, fn, fns, timeDurationStr, false)
	if err != nil {
		a.respondWithError(w, err)
		return
	}

	resp, err := json.Marshal(result)
	if err != nil {
		a.respondWithError(w, err)
		return
	}
	a.respondWithSuccess(w, resp)
}


func (a *API) TotalErrRequestCount(w http.ResponseWriter, r *http.Request) {
	//vars := mux.Vars(r)
	//name := vars["configmap"]
	fn := a.extractQueryParamFromRequest(r, "function")
	fns := a.extractQueryParamFromRequest(r, "namespace")
	url := a.extractQueryParamFromRequest(r, "path")
	timeDurationStr := a.extractQueryParamFromRequest(r, "window")
	//timeDuration, err := time.ParseDuration(timeDurationStr)
	//if err != nil {
	//	log.Printf("Error parsing time duration :%v", err)
	//	a.respondWithError(w, err)
	//	return
	//}
	method := a.extractQueryParamFromRequest(r, "method")

	result, err := a.promClient.GetTotalFailedRequestsToFunc(fn, fns, url, method, timeDurationStr, false)
	if err != nil {
		a.respondWithError(w, err)
		return
	}

	resp, err := json.Marshal(result)
	if err != nil {
		a.respondWithError(w, err)
		return
	}
	a.respondWithSuccess(w, resp)
}

