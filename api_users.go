package main

// ACL users declared beside the default user, each with its password in
// a Secret. plans/completed/01-high-availability.md describes them.

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"
)

// RedisUser is one ACL user. Rules are ACL rules as ACL SETUSER and
// ACL LIST write them, separated by spaces.
type RedisUser struct {
	Name     string       `json:"name"`
	Password SecretKeyRef `json:"password"`
	Rules    string       `json:"rules"`
}

// setUser is the command that makes the server's user match this one,
// whatever it was before: reset clears every permission and password,
// and the rest grants what the spec says. The password goes as its
// SHA-256, so the plaintext never reaches the server's memory or a
// MONITOR stream.
func (u RedisUser) setUser(password string) []string {
	sum := sha256.Sum256([]byte(password))
	return append([]string{"ACL", "SETUSER", u.Name, "reset", "on", "#" + hex.EncodeToString(sum[:])}, aclRules(u.Rules)...)
}

// aclRules splits rules into the arguments ACL SETUSER takes: one for
// each rule, with a selector in parentheses kept whole, spaces and all.
// A YAML block scalar can break the rules across lines, so any white
// space separates them.
func aclRules(rules string) []string {
	var list []string
	var current strings.Builder
	depth := 0
	for _, r := range rules {
		switch {
		case r == '(':
			depth++
		case r == ')' && depth > 0:
			depth--
		case unicode.IsSpace(r) && depth == 0:
			if current.Len() > 0 {
				list = append(list, current.String())
				current.Reset()
			}
			continue
		}
		current.WriteRune(r)
	}
	if current.Len() > 0 {
		list = append(list, current.String())
	}
	return list
}

// credentialSecrets names the Secrets the instance managers read for
// the users' passwords and the server's certificate.
func (s RedisSpec) credentialSecrets(redis string) []string {
	var names []string
	for _, user := range s.Users {
		names = append(names, user.Password.Secret)
	}
	if secret := s.tlsSecret(redis); secret != "" {
		names = append(names, secret)
	}
	return names
}
