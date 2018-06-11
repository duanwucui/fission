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

package client

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func (c *Client) TotalRequestToUrlGet(url, method, window, function, namespace string) (float64, error) {
	relativeUrl := "metrics/requests"
	relativeUrl += fmt.Sprintf("?url=%v&method=%v&window=%v&function=%v&namespace=%v", url, method, window, function, namespace)

	resp, err := http.Get(c.url(relativeUrl))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, err := c.handleResponse(resp)
	if err != nil {
		return 0, err
	}

	var result float64
	err = json.Unmarshal(body, &result)
	if err != nil {
		return 0, err
	}

	return result, nil
}

func (c *Client) TotalErrorRequestToFuncGet(function, namespace, window, url, method string) (float64, error) {
	relativeUrl := "metrics/error-requests"
	relativeUrl += fmt.Sprintf("?function=%v&namespace=%v&window=%v&path=%v&method=%v", function, namespace, window, url, method)

	resp, err := http.Get(c.url(relativeUrl))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, err := c.handleResponse(resp)
	if err != nil {
		return 0, err
	}

	var result float64
	err = json.Unmarshal(body, &result)
	if err != nil {
		return 0, err
	}

	return result, nil
}
