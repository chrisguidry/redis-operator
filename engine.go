package main

// The engines this operator runs. Redis and Valkey speak the same
// protocol and take the same configuration, and differ in the name of
// their server binary and the image that ships it.

func serverBinary(engine string) string {
	if engine == "valkey" {
		return "valkey-server"
	}
	return "redis-server"
}
