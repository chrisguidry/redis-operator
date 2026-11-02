package main

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// errorText is an error's text for a log line or a condition message.
// It keeps the source's text word for word, and drops the newline that
// ends a JSON body, so a log line stays one line. A network error
// leaves out the local and remote addresses: the local port changes on
// every connection, and a message that changes on every failure writes
// a new status each time.
func errorText(err error) string {
	var request *url.Error
	var network *net.OpError
	if errors.As(err, &request) && errors.As(err, &network) {
		return fmt.Sprintf("%s %q: %s %s: %s", request.Op, request.URL, network.Op, network.Net, strings.TrimSpace(network.Err.Error()))
	}
	return strings.TrimSpace(err.Error())
}
