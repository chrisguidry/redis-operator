package main

// The name of each request to the API server, for the count of requests
// that the operator and each instance manager export. The name is the
// Kubernetes verb, the resource with any subresource, and the code the
// API server answered, as the API server's own request metrics name
// them, so the two can be read side by side.

import (
	"net/http"
	"strconv"
	"strings"
)

// requestVerb is the Kubernetes verb of a request: a GET of a
// collection is a list, or a watch when it asks for one.
func requestVerb(method, path string, watch bool) string {
	switch method {
	case http.MethodGet:
		_, name := requestResource(path)
		switch {
		case watch:
			return "watch"
		case name == "":
			return "list"
		}
		return "get"
	case http.MethodPost:
		return "create"
	case http.MethodPut:
		return "update"
	}
	return strings.ToLower(method)
}

// requestResource reads a path that kubeResource.path writes, and
// returns the resource with its subresource, such as "redis/status",
// and the object's name, which is empty for a collection.
func requestResource(path string) (string, string) {
	path, _, _ = strings.Cut(path, "?")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	switch {
	case len(parts) >= 3 && parts[0] == "api":
		parts = parts[2:]
	case len(parts) >= 4 && parts[0] == "apis":
		parts = parts[3:]
	default:
		return "unknown", ""
	}
	if len(parts) >= 3 && parts[0] == "namespaces" {
		parts = parts[2:]
	}
	switch len(parts) {
	case 1:
		return parts[0], ""
	case 2:
		return parts[0], parts[1]
	}
	return parts[0] + "/" + parts[2], parts[1]
}

// requestCode is the answer's code, or "error" for a request that got
// no answer.
func requestCode(resp *http.Response, err error) string {
	if err != nil {
		return "error"
	}
	return strconv.Itoa(resp.StatusCode)
}
