// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	mkapi "github.com/gardener/scaling-advisor/api/minkapi"
	"github.com/gardener/scaling-advisor/minkapi/server/typeinfo"
	"github.com/gardener/scaling-advisor/minkapi/server/view"

	commontypes "github.com/gardener/scaling-advisor/api/common/types"
	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type RequestParams struct {
	Method      string
	Target      string
	ContentType string
}

func TestHTTPHandlers(t *testing.T) {
	s, mux, err := startMinkapiService(t)
	if err != nil {
		t.Errorf("Can not start minkapi service: %v", err)
		return
	}

	tests := map[string]struct {
		filePath                         string
		expectedStatus                   int
		reqParams                        RequestParams
		ignoredFieldsForOutputComparison cmp.Option
	}{
		"fetch existing pod": {
			filePath: "./testdata/pod-a.json",
			reqParams: RequestParams{
				Method:      http.MethodGet,
				Target:      "/api/v1/namespaces/default/pods/bingo",
				ContentType: "application/json",
			},
			expectedStatus:                   http.StatusOK,
			ignoredFieldsForOutputComparison: cmpopts.IgnoreFields(corev1.Pod{}, "ResourceVersion"),
		},
		"delete existing pod": {
			filePath: "./testdata/pod-a.json",
			reqParams: RequestParams{
				Method:      http.MethodDelete,
				Target:      "/api/v1/namespaces/default/pods/bingo",
				ContentType: "application/json",
			},
			expectedStatus: http.StatusOK,
		},
		"erroneous label selector for pods": {
			filePath: "./testdata/pod-a.json",
			reqParams: RequestParams{
				Method:      http.MethodGet,
				Target:      "/api/v1/pods?labelSelector=app.kubernetes.io/name=*?",
				ContentType: "application/json",
			},
			expectedStatus: http.StatusBadRequest,
		},
		"fetch pod list": {
			filePath: "./testdata/pod-a.json",
			reqParams: RequestParams{
				Method:      http.MethodGet,
				Target:      "/api/v1/namespaces/default/pods",
				ContentType: "application/json",
			},
			expectedStatus:                   http.StatusOK,
			ignoredFieldsForOutputComparison: cmpopts.IgnoreFields(corev1.Pod{}, "ResourceVersion"),
		},
		"matching label selector for pods": {
			filePath: "./testdata/pod-a.json",
			reqParams: RequestParams{
				Method:      http.MethodGet,
				Target:      "/api/v1/pods?labelSelector=app.kubernetes.io/component=minkapitest",
				ContentType: "application/json",
			},
			expectedStatus:                   http.StatusOK,
			ignoredFieldsForOutputComparison: cmpopts.IgnoreFields(corev1.Pod{}, "ResourceVersion"),
		},
		"non-matching label selector for pods": {
			filePath: "./testdata/pod-a.json",
			reqParams: RequestParams{
				Method:      http.MethodGet,
				Target:      "/api/v1/pods?labelSelector=app.kubernetes.io/component=abcdefgh",
				ContentType: "application/json",
			},
			expectedStatus:                   http.StatusOK,
			ignoredFieldsForOutputComparison: cmpopts.IgnoreFields(corev1.Pod{}, "ResourceVersion"),
		},
		"create pod binding": {
			filePath: "./testdata/binding-pod-a.json",
			reqParams: RequestParams{
				Method:      http.MethodPost,
				Target:      "/api/v1/namespaces/default/pods/bingo/binding",
				ContentType: "application/json",
			},
			expectedStatus:                   http.StatusOK,
			ignoredFieldsForOutputComparison: cmpopts.IgnoreFields(corev1.Pod{}, "ResourceVersion"),
		},
	}

	t.Cleanup(func() { s.Stop(t.Context()) })
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Cleanup(func() { s.baseView.Reset() })

			if _, err := createObjectFromFileName[corev1.Pod](t, s, "./testdata/pod-a.json", typeinfo.PodsDescriptor.GVK); err != nil {
				t.Errorf("Error creating test object: %v", err)
			}

			jsonData, req := getRequestAndData(t, tc.filePath, tc.reqParams)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			resp := w.Result()
			defer resp.Body.Close()

			if err = compareHTTPHandlerResponse(t, s, resp, tc.reqParams, tc.ignoredFieldsForOutputComparison, jsonData, tc.expectedStatus); err != nil {
				t.Errorf("Failed: %v", err)
			}
		})
	}
}

func TestAPIHandlerMethods(t *testing.T) {
	s, _, err := startMinkapiService(t)
	if err != nil {
		t.Errorf("Can not start minkapi service: %v", err)
		return
	}

	tests := map[string]struct {
		reqParams      RequestParams
		expectedStatus int
		want           any
		handlerFunc    http.HandlerFunc
	}{
		"invalid request for api groups": {
			reqParams: RequestParams{
				Method:      http.MethodPost,
				Target:      "/apis",
				ContentType: "application/json",
			},
			expectedStatus: http.StatusMethodNotAllowed,
			want:           typeinfo.SupportedAPIGroups,
			handlerFunc:    s.handleAPIGroups,
		},
		"get request for api groups": {
			reqParams: RequestParams{
				Method:      http.MethodGet,
				Target:      "/apis",
				ContentType: "application/json",
			},
			expectedStatus: http.StatusOK,
			want:           typeinfo.SupportedAPIGroups,
			handlerFunc:    s.handleAPIGroups,
		},
		"invalid request for api versions": {
			reqParams: RequestParams{
				Method:      http.MethodPost,
				Target:      "/api",
				ContentType: "application/json",
			},
			expectedStatus: http.StatusMethodNotAllowed,
			want:           typeinfo.SupportedAPIVersions,
			handlerFunc:    s.handleAPIVersions,
		},
		"get request for api versions": {
			reqParams: RequestParams{
				Method:      http.MethodGet,
				Target:      "/api",
				ContentType: "application/json",
			},
			expectedStatus: http.StatusOK,
			want:           typeinfo.SupportedAPIVersions,
			handlerFunc:    s.handleAPIVersions,
		},
		"invalid request for api resources": {
			reqParams: RequestParams{
				Method:      http.MethodPost,
				Target:      "/api/v1/",
				ContentType: "application/json",
			},
			expectedStatus: http.StatusMethodNotAllowed,
			want:           typeinfo.SupportedCoreAPIResourceList,
		},
		"get request for api resources": {
			reqParams: RequestParams{
				Method:      http.MethodGet,
				Target:      "/api/v1/",
				ContentType: "application/json",
			},
			expectedStatus: http.StatusOK,
			want:           typeinfo.SupportedCoreAPIResourceList,
		},
	}
	t.Cleanup(func() { s.Stop(t.Context()) })
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(tc.reqParams.Method, tc.reqParams.Target, nil)
			req.Header.Set("Content-Type", tc.reqParams.ContentType)
			w := httptest.NewRecorder()
			if tc.reqParams.Target == "/api/v1/" {
				testFunc := s.handleAPIResources(tc.want.(metav1.APIResourceList))
				testFunc(w, req)
			} else {
				tc.handlerFunc(w, req)
			}
			resp := w.Result()
			defer resp.Body.Close()

			responseData, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Errorf("expected error to be nil got %v", err)
				return
			}

			if resp.StatusCode != tc.expectedStatus {
				t.Errorf("Unexpected status code, got: %s, expected: %d", resp.Status, tc.expectedStatus)
				t.Logf(">>> Got response: %s\n", string(responseData))
				return
			} else if resp.StatusCode != http.StatusOK {
				t.Logf("Expected status: %s", resp.Status)
				return
			}

			validateAPIResponse(t, tc.reqParams.Target, tc.want, responseData)
		})
	}
}

func validateAPIResponse(t *testing.T, target string, want any, responseData []byte) {
	var got any
	switch target {
	case "/apis":
		got, _ = convertJSONtoObject[metav1.APIGroupList](t, responseData)
	case "/api":
		got, _ = convertJSONtoObject[metav1.APIVersions](t, responseData)
	case "/api/v1/":
		got, _ = convertJSONtoObject[metav1.APIResourceList](t, responseData)
	}
	if diff := cmp.Diff(want, got, nil); diff != "" {
		t.Errorf("object mismatch (-want +got):\n%s", diff)
		return
	} else {
		t.Logf("Got expected output")
	}

}

func TestPatchPutHTTPHandlers(t *testing.T) {
	s, mux, err := startMinkapiService(t)
	if err != nil {
		t.Errorf("Can not start minkapi service: %v", err)
		return
	}
	var testPodPatchStatus = `
{
  "status" : {
	"conditions" : [ {
	  "lastProbeTime" : null,
	  "lastTransitionTime" : "2025-05-08T08:21:44Z",
	  "message" : "no nodes available to schedule pods",
	  "reason" : "Unschedulable",
	  "status" : "False",
	  "type" : "PodScheduled"
	} ]
  }
}
`
	var testPatchName = `{"metadata":{"name": "pwned"}}`
	var testPatchLabel = `{"metadata":{"labels": {"test": "label"}}}`
	var corruptedPatch = `{}}`
	data, _ := os.ReadFile("./testdata/corrupt-pod-a.json")
	var corruptedPodResource = string(data)
	data, _ = os.ReadFile("./testdata/update-pod-a.json")
	var updatedPodResource = string(data)

	patchTests := map[string]struct {
		patchData                        string
		reqParams                        RequestParams
		expectedStatus                   int
		ignoredFieldsForOutputComparison cmp.Option
	}{
		"patch pod status": {
			patchData: testPodPatchStatus,
			reqParams: RequestParams{
				Method:      http.MethodPatch,
				Target:      "/api/v1/namespaces/default/pods/bingo/status",
				ContentType: "application/strategic-merge-patch+json",
			},
			expectedStatus:                   http.StatusOK,
			ignoredFieldsForOutputComparison: cmpopts.IgnoreFields(corev1.Pod{}, "ResourceVersion", "Status.Conditions"),
		},
		"patch pod status with unsupported content type": {
			patchData: testPodPatchStatus,
			reqParams: RequestParams{
				Method:      http.MethodPatch,
				Target:      "/api/v1/namespaces/default/pods/bingo/status",
				ContentType: "application/json-patch+json",
			},
			expectedStatus:                   http.StatusBadRequest,
			ignoredFieldsForOutputComparison: cmpopts.IgnoreFields(corev1.Pod{}, "ResourceVersion", "Status.Conditions"),
		},
		"patch pod name": {
			patchData: testPatchName,
			reqParams: RequestParams{
				Method:      http.MethodPatch,
				Target:      "/api/v1/namespaces/default/pods/bingo",
				ContentType: "application/strategic-merge-patch+json",
			},
			expectedStatus:                   http.StatusUnprocessableEntity,
			ignoredFieldsForOutputComparison: cmpopts.IgnoreFields(corev1.Pod{}, "ResourceVersion", "Name"),
		},
		"patch pod ": {
			patchData: testPatchLabel,
			reqParams: RequestParams{
				Method:      http.MethodPatch,
				Target:      "/api/v1/namespaces/default/pods/bingo",
				ContentType: "application/strategic-merge-patch+json",
			},
			expectedStatus:                   http.StatusOK,
			ignoredFieldsForOutputComparison: cmpopts.IgnoreFields(corev1.Pod{}, "ResourceVersion", "Labels"),
		},
		"patch pod with unsupported content type": {
			patchData: testPatchName,
			reqParams: RequestParams{
				Method:      http.MethodPatch,
				Target:      "/api/v1/namespaces/default/pods/bingo",
				ContentType: "application/json-patch+json",
			},
			expectedStatus:                   http.StatusBadRequest,
			ignoredFieldsForOutputComparison: cmpopts.IgnoreFields(corev1.Pod{}, "ResourceVersion", "Name"),
		},
		"corrupted patch pod": {
			patchData: corruptedPatch,
			reqParams: RequestParams{
				Method:      http.MethodPatch,
				Target:      "/api/v1/namespaces/default/pods/bingo",
				ContentType: "application/strategic-merge-patch+json",
			},
			expectedStatus: http.StatusInternalServerError, // FIXME why should this return internal server error
		},
		"corrupted patch pod status": {
			patchData: corruptedPatch,
			reqParams: RequestParams{
				Method:      http.MethodPatch,
				Target:      "/api/v1/namespaces/default/pods/bingo/status",
				ContentType: "application/strategic-merge-patch+json",
			},
			expectedStatus: http.StatusInternalServerError, // FIXME why should this return internal server error
		},
		"update with corrupted object": {
			patchData: corruptedPodResource,
			reqParams: RequestParams{
				Method:      http.MethodPut,
				Target:      "/api/v1/namespaces/default/pods/bingo",
				ContentType: "application/json",
			},
			expectedStatus: http.StatusBadRequest,
		},
		// "update with new object": {
		// 	patchData: updatedPodResource,
		// 	reqParams: RequestParams{
		// 		Method:      http.MethodPut,
		// 		Target:      "/api/v1/namespaces/default/pods/bingo",
		// 		ContentType: "application/json",
		// 	},
		// 	expectedStatus:                   http.StatusOK,
		// 	ignoredFieldsForOutputComparison: cmpopts.IgnoreFields(corev1.Pod{}, "ResourceVersion", "Name"),
		// },
		"update with new object changing name": {
			patchData: updatedPodResource,
			reqParams: RequestParams{
				Method:      http.MethodPut,
				Target:      "/api/v1/namespaces/default/pods/bingo",
				ContentType: "application/json",
			},
			expectedStatus:                   http.StatusUnprocessableEntity,
			ignoredFieldsForOutputComparison: cmpopts.IgnoreFields(corev1.Pod{}, "ResourceVersion", "Name"),
		},
	}

	t.Cleanup(func() { s.Stop(t.Context()) })
	for name, tc := range patchTests {
		t.Run(name, func(t *testing.T) {
			t.Cleanup(func() { s.baseView.Reset() })

			jsonData, err := os.ReadFile("./testdata/pod-a.json")
			if err != nil {
				t.Logf("failed to read: %v", err)
				return
			}
			if _, err := createObjectFromFileName[corev1.Pod](t, s, "./testdata/pod-a.json", typeinfo.PodsDescriptor.GVK); err != nil {
				t.Errorf("Error creating test object: %v", err)
			}

			testObj := bytes.NewReader([]byte(tc.patchData))
			req := httptest.NewRequest(tc.reqParams.Method, tc.reqParams.Target, testObj)
			req.Header.Set("Content-Type", tc.reqParams.ContentType)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			resp := w.Result()
			defer resp.Body.Close()

			if err = compareHTTPHandlerResponse(t, s, resp, tc.reqParams, tc.ignoredFieldsForOutputComparison, jsonData, tc.expectedStatus); err != nil {
				t.Errorf("Failed: %v", err)
			}
		})
	}
}

func TestPatchPutNoObject(t *testing.T) {
	s, mux, err := startMinkapiService(t)
	if err != nil {
		t.Errorf("Can not start minkapi service: %v", err)
		return
	}
	var testPodPatchStatus = `
{
  "status" : {
	"conditions" : [ {
	  "lastProbeTime" : null,
	  "lastTransitionTime" : "2025-05-08T08:21:44Z",
	  "message" : "no nodes available to schedule pods",
	  "reason" : "Unschedulable",
	  "status" : "False",
	  "type" : "PodScheduled"
	} ]
  }
}
`
	var testPatchName = `{"metadata":{"name": "pwned"}}`

	patchTests := map[string]struct {
		patchData                        string
		reqParams                        RequestParams
		expectedStatus                   int
		ignoredFieldsForOutputComparison cmp.Option
	}{
		"patch status of non-existent pod": {
			patchData: testPodPatchStatus,
			reqParams: RequestParams{
				Method:      http.MethodPatch,
				Target:      "/api/v1/namespaces/default/pods/bingoz/status",
				ContentType: "application/strategic-merge-patch+json",
			},
			expectedStatus: http.StatusNotFound,
		},
		"patch non-existent pod": {
			patchData: testPatchName,
			reqParams: RequestParams{
				Method:      http.MethodPatch,
				Target:      "/api/v1/namespaces/default/pods/bingoz",
				ContentType: "application/strategic-merge-patch+json",
			},
			expectedStatus: http.StatusNotFound,
		},
	}

	t.Cleanup(func() { s.Stop(t.Context()) })
	for name, tc := range patchTests {
		t.Run(name, func(t *testing.T) {
			t.Cleanup(func() { s.baseView.Reset() })

			jsonData, err := os.ReadFile("./testdata/pod-a.json")
			if err != nil {
				t.Logf("failed to read: %v", err)
				return
			}

			testObj := bytes.NewReader([]byte(tc.patchData))
			req := httptest.NewRequest(tc.reqParams.Method, tc.reqParams.Target, testObj)
			req.Header.Set("Content-Type", tc.reqParams.ContentType)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			resp := w.Result()
			defer resp.Body.Close()

			if err = compareHTTPHandlerResponse(t, s, resp, tc.reqParams, tc.ignoredFieldsForOutputComparison, jsonData, tc.expectedStatus); err != nil {
				t.Errorf("Failed: %v", err)
			}
		})
	}
}

func TestNoObject(t *testing.T) {
	s, mux, err := startMinkapiService(t)
	if err != nil {
		t.Errorf("Can not start minkapi service: %v", err)
		return
	}
	tests := map[string]struct {
		filePath                         string
		expectedStatus                   int
		reqParams                        RequestParams
		ignoredFieldsForOutputComparison cmp.Option
	}{
		"pod creation": {
			filePath: "./testdata/pod-a.json",
			reqParams: RequestParams{
				Method:      http.MethodPost,
				Target:      "/api/v1/namespaces/default/pods",
				ContentType: "application/json",
			},
			expectedStatus:                   http.StatusOK,
			ignoredFieldsForOutputComparison: cmpopts.IgnoreFields(corev1.Pod{}, "ResourceVersion"),
		},
		"invalid request target": {
			filePath: "./testdata/pod-a.json",
			reqParams: RequestParams{
				Method:      http.MethodPost,
				Target:      "/api/v1/namespaces/default/pods/bingo",
				ContentType: "application/json",
			},
			expectedStatus: http.StatusMethodNotAllowed,
		},
		"create corrupted pod": {
			filePath: "./testdata/corrupt-pod-a.json",
			reqParams: RequestParams{
				Method:      http.MethodPost,
				Target:      "/api/v1/namespaces/default/pods",
				ContentType: "application/json",
			},
			expectedStatus: http.StatusBadRequest,
		},
		"create pod without namespace in request target": {
			filePath: "./testdata/pod-a.json",
			reqParams: RequestParams{
				Method:      http.MethodPost,
				Target:      "/api/v1/pods",
				ContentType: "application/json",
			},
			expectedStatus:                   http.StatusOK,
			ignoredFieldsForOutputComparison: cmpopts.IgnoreFields(corev1.Pod{}, "ResourceVersion"),
		},
		"create pod missing name and generateName": {
			filePath: "./testdata/name-miss-pod-a.json",
			reqParams: RequestParams{
				Method:      http.MethodPost,
				Target:      "/api/v1/namespaces/default/pods",
				ContentType: "application/json",
			},
			expectedStatus:                   http.StatusBadRequest,
			ignoredFieldsForOutputComparison: cmpopts.IgnoreFields(corev1.Pod{}, "ResourceVersion"),
		},
		"create pod missing name, UID and creationTimestamp": {
			filePath: "./testdata/uid-ts-pod-a.json",
			reqParams: RequestParams{
				Method:      http.MethodPost,
				Target:      "/api/v1/namespaces/default/pods",
				ContentType: "application/json",
			},
			expectedStatus:                   http.StatusOK,
			ignoredFieldsForOutputComparison: cmpopts.IgnoreFields(corev1.Pod{}, "ResourceVersion", "Name", "Namespace", "UID", "CreationTimestamp"),
		},
		"fetch non-existent pod": {
			filePath: "./testdata/pod-a.json",
			reqParams: RequestParams{
				Method:      http.MethodGet,
				Target:      "/api/v1/namespaces/default/pods/bingo",
				ContentType: "application/json",
			},
			expectedStatus: http.StatusNotFound,
		},
		"delete non-existent pod": {
			filePath: "./testdata/pod-a.json",
			reqParams: RequestParams{
				Method:      http.MethodDelete,
				Target:      "/api/v1/namespaces/default/pods/bingo",
				ContentType: "application/json",
			},
			expectedStatus: http.StatusNotFound,
		},
		"update non-existent pod": {
			filePath: "./testdata/pod-a.json",
			reqParams: RequestParams{
				Method:      http.MethodPut,
				Target:      "/api/v1/namespaces/default/pods/bingo",
				ContentType: "application/json",
			},
			expectedStatus: http.StatusNotFound,
		},
	}

	t.Cleanup(func() { s.Stop(t.Context()) })
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Cleanup(func() { s.baseView.Reset() })

			jsonData, req := getRequestAndData(t, tc.filePath, tc.reqParams)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			resp := w.Result()
			defer resp.Body.Close()

			if err = compareHTTPHandlerResponse(t, s, resp, tc.reqParams, tc.ignoredFieldsForOutputComparison, jsonData, tc.expectedStatus); err != nil {
				t.Errorf("Failed: %v", err)
			}
		})
	}

}

func TestWatch(t *testing.T) {
	s, mux, err := startMinkapiService(t)
	if err != nil {
		t.Errorf("Can not start minkapi service: %v", err)
		return
	}

	tests := map[string]struct {
		filePath       string
		expectedStatus int
		reqParams      RequestParams
	}{
		"watch all pods": {
			filePath: "./testdata/pod-a.json",
			reqParams: RequestParams{
				Method:      http.MethodGet,
				Target:      "/api/v1/pods?watch=1&resourceVersion=0",
				ContentType: "application/json",
			},
			expectedStatus: http.StatusOK,
		},
	}

	t.Cleanup(func() { s.Stop(t.Context()) })
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Cleanup(func() { s.baseView.Reset() })

			if _, err := createObjectFromFileName[corev1.Pod](t, s, "./testdata/pod-a.json", typeinfo.PodsDescriptor.GVK); err != nil {
				t.Errorf("Error creating test object: %v", err)
			}

			wantData, req := getRequestAndData(t, tc.filePath, tc.reqParams)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			resp := w.Result()
			defer resp.Body.Close()

			if err = handleTestWatchResponse(t, resp, wantData); err != nil {
				t.Errorf("Could not get watch response: %v", err)
				return
			}

			if resp.StatusCode != tc.expectedStatus {
				t.Errorf("Unexpected status code, got: %d, expected: %d", resp.StatusCode, tc.expectedStatus)
				return
			} else if resp.StatusCode != http.StatusOK {
				t.Logf("Expected status: %d", tc.expectedStatus)
				return
			}
		})
	}
}

// -- Helper functions ------------------------------------------------------------------------

func handleTestWatchResponse(t *testing.T, resp *http.Response, wantData []byte) error {
	t.Helper()
	scanner := bufio.NewScanner(resp.Body)
	eventCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		got, eventType, err := parseWatchEvent(t, line)
		if err != nil {
			t.Logf("Failed to parse watch event: %v", err)
			continue
		}
		want, _ := convertJSONtoObject[corev1.Pod](t, wantData)
		if diff := cmp.Diff(want, *got, cmpopts.IgnoreFields(corev1.Pod{}, "ResourceVersion")); diff != "" {
			t.Logf(">>> want\n%s\n", string(wantData))
			t.Logf(">>> got\n%s\n", line)
			t.Errorf("WATCH object mismatch (-want +got):\n%s", diff)
			return err
		}

		t.Logf("Watch event: %s got %s/%s, resourceVersion: %s", eventType, got.Namespace, got.Name, got.ResourceVersion)
		eventCount++
	}
	if scanner.Err() != nil {
		return scanner.Err()
	}
	if eventCount == 0 {
		respData, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("No watch events received, response: %q", string(respData))
	}
	return nil
}

func parseWatchEvent(t *testing.T, line string) (*corev1.Pod, string, error) {
	t.Helper()
	var rawEvent struct {
		Type   string          `json:"type"`
		Object json.RawMessage `json:"object"`
	}

	if err := json.Unmarshal([]byte(line), &rawEvent); err != nil {
		return nil, "", fmt.Errorf("failed to unmarshal event: %w", err)
	}
	var pod corev1.Pod
	if err := json.Unmarshal(rawEvent.Object, &pod); err != nil {
		t.Logf("Response event object:\n%s\n", rawEvent)
		return nil, "", fmt.Errorf("failed to unmarshal pod: %w", err)
	}

	return &pod, rawEvent.Type, nil
}

func getRequestType(t *testing.T, reqMethod, reqTarget, resourceName string) string {
	t.Helper()
	if idx := strings.Index(reqTarget, "watch="); idx != -1 {
		return "WATCH"
	}
	if idx := strings.Index(reqTarget, "/binding"); idx != -1 {
		return "BIND"
	}
	idx := strings.Index(reqTarget, "/"+resourceName) // FIXME what about label selector reqTarget
	if idx != -1 && (reqTarget == reqTarget[:idx+1+len(resourceName)] || strings.Contains(reqTarget, resourceName+"?")) {
		if reqMethod == http.MethodGet {
			return "LIST"
		} else if reqMethod != http.MethodPost {
			return "UNKNOWN"
		}
	}
	return reqMethod
}

func handlePodDeletionResponse(t *testing.T, s *InMemoryKAPI, wantData []byte) error {
	t.Helper()
	wantPod, _ := convertJSONtoObject[corev1.Pod](t, wantData)
	p, err := s.baseView.ListPods(mkapi.MatchCriteria{Namespace: wantPod.Namespace, Names: sets.New(wantPod.Name)})
	if err != nil {
		return fmt.Errorf("Error listing pods")
	}
	if len(p) != 0 {
		return fmt.Errorf("Pod deletion unsuccesful")
	}
	return nil
}

func handlePodBindingResponse(t *testing.T, s *InMemoryKAPI, responseData, wantData []byte) error {
	t.Helper()
	gotStatus, _ := convertJSONtoObject[metav1.Status](t, responseData)
	if gotStatus.Status != metav1.StatusSuccess {
		return fmt.Errorf("Pod binding unsuccessful")
	}
	wantPodBind, _ := convertJSONtoObject[corev1.Binding](t, wantData)
	p, err := s.baseView.ListPods(wantPodBind.Namespace, []string{wantPodBind.Name}...)
	if err != nil {
		return fmt.Errorf("Error listing pods")
	}
	if len(p) == 0 {
		return fmt.Errorf("Pod not found")
	}
	if p[0].Spec.NodeName == wantPodBind.Target.Name {
		t.Logf("Pod binding successful: nodeName is %s", p[0].Spec.NodeName)
	} else {
		return fmt.Errorf("Pod binding unsuccessful")
	}
	return nil
}

func compareHTTPHandlerResponse(t *testing.T, s *InMemoryKAPI, resp *http.Response, params RequestParams, ignoredFieldsForOutputComparison cmp.Option, wantData []byte, expectedStatus int) (err error) {
	t.Helper()
	var (
		got  corev1.Pod
		want any
	)
	reqType := getRequestType(t, params.Method, params.Target, "pods")

	responseData, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("Could not get response body: %v", err)
	}

	if resp.StatusCode != expectedStatus {
		return fmt.Errorf("Unexpected status code, got: %s, expected: %d", resp.Status, expectedStatus)
	} else if resp.StatusCode != http.StatusOK {
		t.Logf("Expected status: %s", resp.Status)
		return nil
	}

	switch reqType {
	case "DELETE":
		if err := handlePodDeletionResponse(t, s, wantData); err != nil {
			return err
		}
		return nil

	case "BIND":
		if err := handlePodBindingResponse(t, s, responseData, wantData); err != nil {
			return err
		}
		return nil

	case "LIST":
		gotList, err := convertJSONtoObject[corev1.PodList](t, responseData)
		if err != nil {
			return fmt.Errorf("error converting response body to podlist: %v", err)
		}
		if len(gotList.Items) == 0 {
			t.Logf("No elements found for the requested LIST")
			return nil
		}
		got = gotList.Items[0]

	default:
		got, err = convertJSONtoObject[corev1.Pod](t, responseData)
		if err != nil {
			return fmt.Errorf("error converting response body to pod object: %v", err)
		}
	}

	want, _ = convertJSONtoObject[corev1.Pod](t, wantData)

	if diff := cmp.Diff(want, got, ignoredFieldsForOutputComparison); diff != "" {
		t.Logf(">>> want\n%s\n", string(wantData))
		t.Logf(">>> got\n%s\n", string(responseData))
		t.Errorf("%s object mismatch (-want +got):\n%s", reqType, diff)
		return err
	}
	// t.Cleanup(func() { s.baseView.Reset() })

	return nil
}

func createObjectFromFileName[T any](t *testing.T, svc *InMemoryKAPI, fileName string, gvk schema.GroupVersionKind) (T, error) {
	t.Helper()
	var (
		jsonData []byte
		obj      T
		err      error
	)
	jsonData, err = os.ReadFile(fileName)
	if err != nil {
		return obj, err
	}
	obj, err = convertJSONtoObject[T](t, jsonData)
	if err != nil {
		return obj, err
	}
	objInterface, ok := any(&obj).(metav1.Object)
	if !ok {
		return obj, err
	}
	err = svc.baseView.CreateObject(gvk, objInterface)
	if err != nil {
		return obj, err
	}
	t.Logf("Creating %s %s", gvk.Kind, objInterface.GetName())
	return obj, nil
}

func startMinkapiService(t *testing.T) (*InMemoryKAPI, *http.ServeMux, error) { // Need this explicitly in order to get baseViewMux
	t.Helper()
	var err error
	cfg := mkapi.Config{
		BasePrefix: mkapi.DefaultBasePrefix,
		ServerConfig: commontypes.ServerConfig{
			HostPort:       commontypes.HostPort{Host: "localhost", Port: 9892},
			KubeConfigPath: "/tmp/minkmkapi-test.yaml",
		},
		WatchConfig: mkapi.WatchConfig{
			QueueSize: mkapi.DefaultWatchQueueSize,
			Timeout:   500 * time.Millisecond,
		},
	}
	log := logr.FromContextOrDiscard(t.Context())

	defer func() {
		if err != nil {
			err = fmt.Errorf("%w: %w", mkapi.ErrInitFailed, err)
		}
	}()
	scheme := typeinfo.SupportedScheme
	baseView, err := view.New(log, &mkapi.ViewArgs{
		Name:           mkapi.DefaultBasePrefix,
		KubeConfigPath: cfg.KubeConfigPath,
		Scheme:         scheme,
		WatchConfig:    cfg.WatchConfig,
	})
	if err != nil {
		return nil, nil, err
	}
	rootMux := http.NewServeMux()
	s := &InMemoryKAPI{
		cfg:     cfg,
		scheme:  scheme,
		rootMux: rootMux,
		server: &http.Server{
			Addr:    net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
			Handler: rootMux,
		},
		baseView: baseView,
	}
	baseViewMux := http.NewServeMux()
	s.registerRoutes(log, baseViewMux, baseView)
	return s, baseViewMux, err
}

func getRequestAndData(t *testing.T, filePath string, params RequestParams) (jsonData []byte, req *http.Request) {
	t.Helper()
	jsonData, err := os.ReadFile(filePath)
	if err != nil {
		t.Logf("failed to read: %v", err)
		return
	}

	requestData := bytes.NewReader(jsonData)
	req = httptest.NewRequest(params.Method, params.Target, requestData)
	req.Header.Set("Content-Type", params.ContentType)
	return
}

func convertJSONtoObject[T any](t *testing.T, data []byte) (T, error) {
	t.Helper()
	var obj T
	if err := json.Unmarshal(data, &obj); err != nil {
		t.Errorf("error unmarshalling JSON: %v", err)
		return obj, err
	}
	return obj, nil
}
