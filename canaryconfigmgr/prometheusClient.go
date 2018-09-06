package canaryconfigmgr

import (
	"fmt"
	"time"
	"golang.org/x/net/context"

	promApi1 "github.com/prometheus/client_golang/api/prometheus"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
)

type PrometheusApiClient struct {
	client promApi1.QueryAPI
}

// TODO  prometheusSvc will need to come from helm chart and passed to controller pod.
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

func(promApi *PrometheusApiClient) GetTotalRequestToFunc(path string, method string, funcName string, funcNs string, window time.Duration, reqsInPrevWindow int) (float64, error) {
	queryString := fmt.Sprintf("fission_function_calls_total{path=\"%s\",method=\"%s\",name=\"%s\",namespace=\"%s\"}[%v]", path, method, funcName, funcNs, window)
	log.Printf("Querying total function calls for : %s ", queryString)

	totalReqs, err := promApi.executeQuery(queryString)
	if err != nil {
		log.Printf("Error executing query : %s, err : %v", queryString, err)
		return 0, err
	}

	reqsInCurrentWindow := totalReqs - float64(reqsInPrevWindow)

	log.Printf("total calls to this function %v method %v : %v", path, method, reqsInCurrentWindow)

	return reqsInCurrentWindow, nil
}

func (promApi *PrometheusApiClient) GetTotalFailedRequestsToFunc(funcName string, funcNs string, path string, method string, window time.Duration, failedReqsInPrevWindow int) (float64, error) {
	queryString := fmt.Sprintf("fission_function_errors_total{name=\"%s\",namespace=\"%s\",path=\"%s\", method=\"%s\"}[%v]", funcName, funcNs, path, method, window)
	log.Printf("Querying fission_function_errors_total qs : %s", queryString)

	totalFailedRequestToFunc, err := promApi.executeQuery(queryString)
	if err != nil {
		log.Printf("Error executing query : %s, err : %v", queryString, err)
		return 0, err
	}

	failedReqsInCurrentWindow := totalFailedRequestToFunc - float64(failedReqsInPrevWindow)
	log.Printf("total failed calls to function: %v.%v : %v", funcName, funcNs, failedReqsInCurrentWindow)

	return failedReqsInCurrentWindow, nil
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