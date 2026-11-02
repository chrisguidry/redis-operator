package main

// Each test here has its own servers, ports, and fake API server, so
// they run in parallel, which keeps the suite within go test's timeout.

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

// usersHarness declares a Redis whose spec names users, with each
// user's password in the Secret app-user under the user's name.
func usersHarness(t *testing.T, engine string, instances int, users ...RedisUser) *managerHarness {
	t.Helper()
	h := startManagerHarnessOn(t, startFakeKube(t), "cache", engine, instances, func(spec *RedisSpec) {
		spec.Users = users
	})
	passwords := map[string][]byte{}
	for _, user := range users {
		passwords[user.Name] = []byte(user.Name + "-password")
	}
	putSecret(h.api, "app-user", passwords)
	return h
}

func appUser(name, rules string) RedisUser {
	return RedisUser{Name: name, Password: SecretKeyRef{Secret: "app-user", Key: name}, Rules: rules}
}

// as runs one command on the server of name, as user with password.
func (h *managerHarness) as(name, user, password string, args ...string) (any, error) {
	conn := h.client(name)
	conn.password = ""
	if _, err := conn.do(context.Background(), "AUTH", user, password); err != nil {
		return nil, err
	}
	return conn.do(context.Background(), args...)
}

func refusedWith(err error, code string) bool {
	var reply replyError
	return errors.As(err, &reply) && reply.code() == code
}

func TestDeclaredUsersReachEveryInstanceAndKeepTheirRulesAcrossASwitchover(t *testing.T) {
	t.Parallel()
	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			h := usersHarness(t, engine, 2, appUser("app", "~app:* +@all -@dangerous"))
			h.setTarget("cache-0")
			h.start("cache-0", "")
			h.start("cache-1", "")
			h.waitFor("cache-0 streams to cache-1", func() bool {
				return h.report("cache-1").LinkUp && h.report("cache-0").SyncedReplicas == 1
			})
			h.setCurrentPrimary("cache-0")
			h.waitFor("app can read its keys on the replica", func() bool {
				_, err := h.as("cache-1", "app", "app-password", "GET", "app:x")
				return err == nil
			})

			h.switchTo("cache-1")
			h.waitFor("cache-1 holds the lease and streams to cache-0", func() bool {
				return h.holder() == "cache-1" && h.report("cache-1").Ready && h.report("cache-1").SyncedReplicas == 1
			})

			_, err := h.as("cache-1", "app", "app-password", "SET", "app:x", "1")
			mustSucceed(t, err)
			_, err = h.as("cache-1", "app", "app-password", "SET", "other:x", "1")
			mustMatch(t, refusedWith(err, "NOPERM"), true)
			_, err = h.as("cache-1", "app", "app-password", "CONFIG", "GET", "maxmemory")
			mustMatch(t, refusedWith(err, "NOPERM"), true)
		})
	}
}

func TestChangesToUsersApplyWithoutARestart(t *testing.T) {
	t.Parallel()
	h := usersHarness(t, "redis", 1, appUser("app", "~app:* +get"), appUser("old", "+ping"))
	h.setTarget("cache-0")
	h.start("cache-0", "")
	h.waitFor("app can read", func() bool {
		_, err := h.as("cache-0", "app", "app-password", "GET", "app:x")
		return err == nil
	})
	_, err := h.client("cache-0").do(context.Background(), "ACL", "SETUSER", "by-hand", "on", ">by-hand", "+ping")
	mustSucceed(t, err)
	before := serverRunID(t, h.client("cache-0"))

	putSecret(h.api, "app-user", map[string][]byte{"app": []byte("rotated"), "old": []byte("old-password")})
	h.api.update(redisResource, testNamespace, "cache", func(object map[string]any) {
		object["spec"].(map[string]any)["users"] = []any{toObject(t, appUser("app", "~app:* +get +set"))}
	})

	h.waitFor("app writes with its new password", func() bool {
		_, err := h.as("cache-0", "app", "rotated", "SET", "app:x", "1")
		return err == nil
	})
	_, err = h.as("cache-0", "app", "app-password", "PING")
	mustMatch(t, refusedWith(err, "WRONGPASS"), true)
	h.waitFor("old is gone", func() bool {
		_, err := h.as("cache-0", "old", "old-password", "PING")
		return refusedWith(err, "WRONGPASS")
	})
	_, err = h.as("cache-0", "by-hand", "by-hand", "PING")
	mustSucceed(t, err)
	mustMatch(t, serverRunID(t, h.client("cache-0")), before)
}

func TestAUserThatCannotBeSetIsLoggedAndTheServerServesAnyway(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		user RedisUser
		want string
	}{
		{"a missing secret", RedisUser{Name: "app", Password: SecretKeyRef{Secret: "absent", Key: "password"}, Rules: "+@all"}, "cannot set user app: reading secret absent"},
		{"a missing key", RedisUser{Name: "app", Password: SecretKeyRef{Secret: "app-user", Key: "absent"}, Rules: "+@all"}, "cannot set user app: secret app-user has no key absent"},
		{"a rule the server refuses", appUser("app", "+nosuchcommand"), "cannot set user app: ERR"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := usersHarness(t, "redis", 1, c.user)
			h.setTarget("cache-0")

			h.start("cache-0", "")

			h.waitFor("cache-0 is a ready primary", func() bool { return h.report("cache-0").Ready })
			lines := h.instances["cache-0"].manager.log.(*syncBuffer).lines()
			found := slices.IndexFunc(lines, func(line string) bool { return strings.Contains(line, c.want) })
			mustMatch(t, found >= 0, true)
			mustMatch(t, len(slices.DeleteFunc(lines, func(line string) bool { return !strings.Contains(line, c.want) })), 1)
		})
	}
}
