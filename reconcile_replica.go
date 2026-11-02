package main

// The operator's part of replica mode: it enters replica mode for a new
// Redis that names a source, reports how far behind the source it is,
// and promotes it when spec.replica.enabled turns false: it pauses
// writes on the source, waits until the holder has every write the
// source accepted, then lets the holder become the primary.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"
)

// sourcePause is how long the source refuses writes during a
// promotion. The promotion waits for the catch-up within it.
const sourcePause = 30 * time.Second

func (d *decision) replica(ctx context.Context) {
	spec := d.r.Spec.Replica
	status := d.status.Replica
	switch {
	case status == nil && spec != nil && spec.Enabled && d.status.CurrentPrimary == "":
		d.status.Replica = &ReplicaStatus{Phase: replicaReplicating}
		d.c.log.printf("%s: replicating from its source", d.subject())
	case status == nil && spec != nil && spec.Enabled:
		d.setCondition("Adopted", ConditionFalse, "AlreadyPrimary", "replica mode starts only on a new Redis; this one has served as a primary")
	case status == nil:
		return
	case status.Phase == replicaReplicating && (spec == nil || !spec.Enabled) && !d.now.Before(d.st.promotionRetryAfter):
		d.startPromotion(ctx)
	case status.Phase == replicaReplicating:
		d.measureLag(ctx)
	case status.Phase == replicaPromoting:
		d.finishPromotion(ctx)
	}
}

// errPasswordUnreadable marks a failure to read the source's password,
// which says nothing about whether the source is up.
var errPasswordUnreadable = errors.New("the source's password could not be read")

// sourceConn connects to the source's primary, with its password.
func (d *decision) sourceConn(ctx context.Context) (*redisConn, string, error) {
	source := d.r.Spec.Replica.Source
	host, port, err := sourceAddress(ctx, source, d.r.Metadata.Namespace)
	if err != nil {
		return nil, "", err
	}
	password := ""
	if source.Password != nil {
		if password, err = readSecretKey(ctx, d.c.client, d.r.Metadata.Namespace, *source.Password); err != nil {
			return nil, "", fmt.Errorf("%w: %w", errPasswordUnreadable, err)
		}
	}
	address := net.JoinHostPort(host, strconv.Itoa(port))
	attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	conn, err := dialRedis(attempt, address, password)
	return conn, address, err
}

func (d *decision) sourceOffset(ctx context.Context) (int64, string, error) {
	conn, address, err := d.sourceConn(ctx)
	if err != nil {
		return 0, "", err
	}
	defer conn.Close()
	attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	info, err := readReplication(attempt, conn)
	return info.offset, address, err
}

func (d *decision) holderOffset() (int64, bool) {
	report, ok := d.st.fresh(d.holder, d.now, 2*d.c.timings.backstop)
	return report.Offset, ok && report.LinkUp
}

func (d *decision) measureLag(ctx context.Context) {
	offset, address, err := d.sourceOffset(ctx)
	if err != nil {
		d.setCondition("SourceReachable", ConditionFalse, "Unreachable", errorText(err))
		return
	}
	d.setCondition("SourceReachable", ConditionTrue, "Reachable", "")
	ours, _ := d.holderOffset()
	d.status.Replica.Source = address
	d.status.Replica.SourceOffset = offset
	d.status.Replica.Lag = max(offset-ours, 0)
}

// startPromotion pauses writes on the source and marks the promotion
// started. A Redis whose spec no longer names the source promotes at
// once, because there is no source to pause, and so does one whose
// source cannot be reached over the network, because a source that is
// down takes no writes to lose. A source that answers, even with an
// error, is still taking writes, so the promotion waits and asks again
// after a backoff. So does one whose password cannot be read, because
// that says nothing about the source.
func (d *decision) startPromotion(ctx context.Context) {
	d.st.promotionStarted = d.now
	if d.r.Spec.Replica == nil {
		d.promote("spec.replica was removed, so the operator promoted without pausing the source")
		return
	}
	conn, address, err := d.sourceConn(ctx)
	_, answered := errors.AsType[replyError](err)
	switch {
	case errors.Is(err, errPasswordUnreadable):
		d.waitForSource("PasswordUnreadable", errorText(err))
		return
	case answered:
		d.waitForSource("SourceRefused", fmt.Sprintf("the source %s answered with an error (%s), so it is up and may still take writes", address, errorText(err)))
		return
	case err != nil:
		d.promote(fmt.Sprintf("the source could not be reached to pause it (%s), so the operator promoted without waiting", errorText(err)))
		return
	}
	defer conn.Close()
	attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := conn.do(attempt, "CLIENT", "PAUSE", strconv.FormatInt(sourcePause.Milliseconds(), 10), "WRITE"); err != nil {
		d.waitForSource("PauseRefused", fmt.Sprintf("the source %s refused CLIENT PAUSE (%s), and it still takes writes", address, errorText(err)))
		return
	}
	d.c.log.printf("%s: promoting: paused writes on the source %s for %s", d.subject(), address, sourcePause)
	d.status.Replica.Phase = replicaPromoting
}

// waitForSource leaves the Redis replicating, because promoting now
// could lose writes the source confirmed, and tries the promotion again
// after a backoff.
func (d *decision) waitForSource(reason, why string) {
	backoff := d.c.timings.switchoverBackoff
	d.c.log.printf("%s: not promoting: %s; trying again in %s", d.subject(), why, backoff)
	d.st.promotionRetryAfter = d.now.Add(backoff)
	d.setCondition("Adopted", ConditionFalse, reason, fmt.Sprintf(
		"%s, so the operator does not promote; it tries again in %s. To promote without pausing the source, remove spec.replica", why, backoff))
}

// finishPromotion promotes once the holder has the source's whole
// offset. When the pause is nearly over without that, the promotion
// goes back to replicating, and the source takes writes again until
// the next attempt.
func (d *decision) finishPromotion(ctx context.Context) {
	offset, _, err := d.sourceOffset(ctx)
	ours, linked := d.holderOffset()
	if err == nil && linked && ours >= offset {
		d.promote(fmt.Sprintf("the holder reached the source's offset %d with its writes paused", offset))
		return
	}
	// The margin leaves the last check inside the pause, so a holder
	// that catches up then is promoted with every write.
	if d.now.Sub(d.st.promotionStarted) <= sourcePause-5*time.Second {
		return
	}
	backoff := d.c.timings.switchoverBackoff
	behind := fmt.Sprintf("the holder was at offset %d and the source at %d", ours, offset)
	switch {
	case err != nil:
		behind = fmt.Sprintf("the source's offset could not be read (%s)", errorText(err))
	case !linked:
		behind = fmt.Sprintf("the holder had no link to the source, at offset %d of the source's %d", ours, offset)
	}
	d.c.log.printf("%s: the holder did not catch up within the source's pause: %s; trying again in %s", d.subject(), behind, backoff)
	d.status.Replica.Phase = replicaReplicating
	d.st.promotionRetryAfter = d.now.Add(backoff)
	d.setCondition("Adopted", ConditionFalse, "CatchUpTimedOut", fmt.Sprintf("the holder did not catch up within the source's pause of %s: %s; the operator pauses the source again in %s", sourcePause, behind, backoff))
}

func (d *decision) promote(message string) {
	d.c.log.printf("%s: promoted: %s", d.subject(), message)
	if d.status.Replica == nil {
		d.status.Replica = &ReplicaStatus{}
	}
	d.status.Replica.Phase = replicaPromoted
	d.setCondition("Adopted", ConditionTrue, "Promoted", message)
}
