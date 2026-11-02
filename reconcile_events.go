package main

// The Events of a Redis's decisions. A decision notes its Event while
// the pass composes the status, and the pass posts the notes, and one
// Event for each condition that transitioned, only once the status
// write lands. A write that a newer version refused lands nothing, and
// the next pass decides again from that version, so an Event posted
// before the write could tell of a decision that never took effect.

import "fmt"

// eventNote is an Event that a decision posts once its status lands.
type eventNote struct {
	kind, reason, message string
}

func (d *decision) note(kind, reason, message string) {
	d.notes = append(d.notes, eventNote{kind, reason, message})
}

// postDecisions posts the notes of a pass, then an Event for each
// condition that is new or whose status or reason changed. A condition
// whose message alone changed, such as the count of ready instances,
// did not transition.
func (c *controller) postDecisions(r *Redis, d *decision) {
	object := referenceTo(redisKind, r.Metadata)
	for _, n := range d.notes {
		c.events.post(object, n.kind, n.reason, n.message)
	}
	for _, next := range d.status.Conditions {
		held, found := findCondition(r.Status.Conditions, next.Type)
		if found && held.Status == next.Status && held.Reason == next.Reason {
			continue
		}
		message := fmt.Sprintf("%s is %s", next.Type, next.Status)
		if next.Message != "" {
			message += ": " + next.Message
		}
		c.events.post(object, conditionEventType(next, r.Status.CurrentPrimary != ""), next.Reason, message)
	}
}

// badStatus is the status of each condition that a person may need to
// act on. A transition to it is a Warning, and any other is Normal.
var badStatus = map[string]ConditionStatus{
	readyCondition:     ConditionFalse,
	"DataLost":         ConditionTrue,
	"SwitchoverFailed": ConditionTrue,
	"RolloutFailed":    ConditionTrue,
	"SourceReachable":  ConditionFalse,
	"Adopted":          ConditionFalse,
}

// conditionEventType is Warning for a transition to a condition's bad
// status, with two exceptions for Ready. A replica that is not ready is
// Normal, because every pod of a rollout restarts that way. A Redis
// with no primary is Normal until its first primary takes the Lease,
// because every new Redis starts that way.
func conditionEventType(c Condition, hadPrimary bool) string {
	if c.Type == readyCondition && (c.Reason == "InstancesNotReady" || c.Reason == "NoPrimary" && !hadPrimary) {
		return eventNormal
	}
	if bad, ok := badStatus[c.Type]; ok && c.Status == bad {
		return eventWarning
	}
	return eventNormal
}

// switchoverCauses says in words why each kind of switchover starts.
var switchoverCauses = map[string]string{
	reasonPodDeleted:   "its pod is being deleted",
	reasonNodeCordoned: "its node is cordoned",
	reasonScaleDown:    "spec.instances no longer keeps it",
	reasonRollout:      "it is the last instance on the old revision",
}

func switchoverMessage(from, to, reason string) string {
	return fmt.Sprintf("moving the primary from %s to %s, because %s (%s)", from, to, switchoverCauses[reason], reason)
}

// observeBackup posts the Event of a Backup that an instance manager
// completed or failed. The operator learns of that only from the
// Backup's phase, so it remembers the phase it last saw of each
// Backup. A Backup it first sees after it starts is remembered without
// an Event, so a restarted operator does not post again every Backup
// that finished before it started. The memory decides only Events,
// never what the operator does.
func (c *controller) observeBackup(backup Backup) {
	uid, phase := backup.Metadata.UID, backup.Status.Phase
	seen, known := c.backupPhases[uid]
	c.backupPhases[uid] = phase
	if !known || seen == phase {
		return
	}
	object := referenceTo(backupKind, backup.Metadata)
	switch phase {
	case backupCompleted:
		c.events.normal(object, "Completed", fmt.Sprintf("%s wrote %s: %d bytes, at offset %d", backup.Status.Instance, backup.Status.Key, backup.Status.Size, backup.Status.Offset))
	case backupFailed:
		c.events.warning(object, "Failed", backup.Status.Error)
	}
}

// forgetBackups forgets the phase of each Backup that is gone.
func (c *controller) forgetBackups(backups []Backup) {
	live := map[string]bool{}
	for _, backup := range backups {
		live[backup.Metadata.UID] = true
	}
	for uid := range c.backupPhases {
		if !live[uid] {
			delete(c.backupPhases, uid)
		}
	}
}
