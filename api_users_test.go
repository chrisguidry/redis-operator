package main

import (
	"slices"
	"testing"
)

func TestRulesSplitOnSpacesAndKeepSelectorsWhole(t *testing.T) {
	t.Parallel()
	cases := []struct {
		rules string
		want  []string
	}{
		{"~app:* +@all -@dangerous", []string{"~app:*", "+@all", "-@dangerous"}},
		{"  +get   ~k  ", []string{"+get", "~k"}},
		{"+@read (~write:* +set) &chan", []string{"+@read", "(~write:* +set)", "&chan"}},
		{"(~a (b) +get) +ping", []string{"(~a (b) +get)", "+ping"}},
		{"+get\n~k\t+set", []string{"+get", "~k", "+set"}},
		{"", nil},
	}
	for _, c := range cases {
		t.Run(c.rules, func(t *testing.T) {
			mustMatch(t, slices.Equal(aclRules(c.rules), c.want), true)
		})
	}
}

func TestSetUserReplacesTheWholeUserWithAHashedPassword(t *testing.T) {
	t.Parallel()
	user := RedisUser{Name: "app", Rules: "~app:* +@all"}

	got := user.setUser("secret")

	mustMatch(t, slices.Equal(got, []string{
		"ACL", "SETUSER", "app", "reset", "on",
		"#2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b",
		"~app:*", "+@all",
	}), true)
}
