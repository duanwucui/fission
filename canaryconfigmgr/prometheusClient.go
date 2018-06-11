package canaryconfigmgr

import (
	"fmt"
	"time"
	"golang.org/x/net/context"

	promApi1 "github.com/prometheus/client_golang/api/prometheus"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
	"math"
)

type PrometheusApiClient struct {
	client promApi1.QueryAPI
}

// TODO  prometheusSvc will need to come from helm chart value and passed to controller pod.
// controllerpod then passes this during canaryConfigMgr create
func MakePrometheusClient(prometheusSvc string) *PrometheusApiClient {
	log.Printf("Making prom client with service : %s", prometheusSvc)
	promApiConfig := promApi1.Config{
		Address: prometheusSvc,
	}

	promApiClient, err := promApi1.New(promApiConfig)
	if err != nil {
		log.Errorf("Error creating prometheus api client for svc : %s, err : %v", prometheusSvc, err)
	}

	apiQueryClient := promApi1.NewQueryAPI(promApiClient)

	log.Printf("Successfully made prom client")
	return &PrometheusApiClient{
		client: apiQueryClient,
	}
}

func(promApi *PrometheusApiClient) GetTotalRequestToFunc(path string, method string, funcName string, funcNs string, window time.Duration) (float64, error) {
	queryString := fmt.Sprintf("fission_function_calls_total{path=\"%s\",method=\"%s\",name=\"%s\",namespace=\"%s\"}[%v]", path, method, funcName, funcNs, window)
	log.Printf("Querying total function calls for : %s ", queryString)

	reqsInCurrentWindow, err := promApi.executeQuery(queryString)
	if err != nil {
		log.Printf("Error executing query : %s, err : %v", queryString, err)
		return 0, err
	}

	queryString = fmt.Sprintf("fission_function_calls_total{path=\"%s\",method=\"%s\",name=\"%s\",namespace=\"%s\"} offset %v", path, method, funcName, funcNs, window + window)
	log.Printf("Querying total function calls for : %s ", queryString)

	reqsInPrevWindow, err := promApi.executeQuery(queryString)
	if err != nil {
		log.Printf("Error executing query : %s, err : %v", queryString, err)
		return 0, err
	}

	totalRequestToUrl := reqsInCurrentWindow - reqsInPrevWindow

	log.Printf("total calls to this url %v method %v : %v", path, method, totalRequestToUrl)

	return totalRequestToUrl, nil
}

func (promApi *PrometheusApiClient) GetTotalFailedRequestsToFunc(funcName string, funcNs string, path string, method string, window time.Duration) (float64, error) {
	queryString := fmt.Sprintf("fission_function_errors_total{name=\"%s\",namespace=\"%s\",path=\"%s\", method=\"%s\"}[%v]", funcName, funcNs, path, method, window)
	log.Printf("Querying fission_function_errors_total qs : %s", queryString)

	failedReqsInCurrentWindow, err := promApi.executeQuery(queryString)
	if err != nil {
		log.Printf("Error executing query : %s, err : %v", queryString, err)
		return 0, err
	}

	queryString = fmt.Sprintf("fission_function_errors_total{name=\"%s\",namespace=\"%s\",path=\"%s\", method=\"%s\"} offset %v", funcName, funcNs, path, method, window + window)
	log.Printf("Querying fission_function_errors_total qs : %s", queryString)

	failedReqsInPrevWindow, err := promApi.executeQuery(queryString)
	if err != nil {
		log.Printf("Error executing query : %s, err : %v", queryString, err)
		return 0, err
	}

	totalFailedRequestToFunc := failedReqsInCurrentWindow - failedReqsInPrevWindow
	log.Printf("total failed calls to function: %v.%v : %v", funcName, funcNs, totalFailedRequestToFunc)

	return totalFailedRequestToFunc, nil
}

func(promApi *PrometheusApiClient) GetFunctionFailurePercentage(path, method, funcName, funcNs string, window time.Duration) (float64, error) {
	// first get a total count of requests to this url in a time window
	totalRequestToUrl, err := promApi.GetTotalRequestToFunc(path, method, funcName, funcNs, window)
	if err != nil {
		return 0, err
	}

	if totalRequestToUrl == 0 {
		return -1, fmt.Errorf("no requests to this url %v and method %v in the window : %v", path, method, window)
	}

	// next, get a total count of errored out requests to this function in the same window
	totalFailedRequestToFunc, err := promApi.GetTotalFailedRequestsToFunc(funcName, funcNs, path, method, window)
	if err != nil {
		return 0, err
	}

	// calculate the failure percentage of the function
	failurePercentForFunc := (totalFailedRequestToFunc / totalRequestToUrl) * 100
	log.Printf("Final failurePercentForFunc for func: %v.%v is %v", funcName, funcNs, failurePercentForFunc)

	return failurePercentForFunc, nil
}

// always get the latest value.
func(promApi *PrometheusApiClient) executeQuery(queryString string) (float64, error) {
	val, err := promApi.client.Query(context.Background(), queryString, time.Now())
	if err != nil {
		// TODO : Add retries in cases of dial tcp error
		log.Errorf("Error querying prometheus qs : %v, err : %v", queryString, err)
		return 0, err
	}

	//log.Printf("Value retrieved from query : %v", val)

	switch {
	case val.Type() == model.ValScalar:
		log.Printf("Value type is scalar")
		scalarVal := val.(*model.Scalar)
		log.Printf("scalarValue : %v", scalarVal.Value)
		return float64(scalarVal.Value), nil

	case val.Type() == model.ValVector:
		log.Printf("value type is vector")
		vectorVal := val.(model.Vector)
		total := float64(0)
		for _, elem := range vectorVal {
			log.Printf("labels : %v, Elem value : %v, timestamp : %v", elem.Metric, elem.Value, elem.Timestamp)
			total = total + float64(elem.Value)
		}
		return total, nil

	case val.Type() == model.ValMatrix:
		//log.Printf("value type is matrix")
		matrixVal := val.(model.Matrix)
		total := float64(0)
		for _, elem := range matrixVal {
				//log.Printf("Only one value, so taking the 0th elem")
				total += float64(elem.Values[len(elem.Values)-1].Value)
		}
		log.Printf("Final total : %v", total)
		return total, nil

	default:
		log.Printf("type unrecognized")
		return 0,nil
	}
}