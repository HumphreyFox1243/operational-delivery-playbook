# Chat Authorisation Boundaries: Per Room Channels Versus One Filtering Channel

TL;DR: Use a separate logical channel for each logistics workspace, authorize every join on the server, and treat presence as leased state that must be rebuilt after reconnect. A single shared channel with server-side filtering can work, but it expands the consequence of one filtering defect and makes membership changes harder to reason about during recovery. Choose it only when you can prove isolation on every publish, replay, and resubscribe path.

The decision rule is operational: if dispatchers, drivers, and partner carriers can lose access independently, make the room the authorization boundary. Transport connections may still be pooled underneath. Do not confuse one socket with one authorization scope.

## Should chat use one filtering channel or a channel per room?

A logistics workspace changes while clients are offline. A driver may leave route `RT-184`, a temporary carrier account may expire, or a dispatcher may move from the night desk to the morning desk. On reconnect, restoring the old subscription set without checking current grants can show an online user in a workspace they may no longer enter. Filtering messages after that stale membership has been restored does not repair the presence leak.

Presence is an assertion with a lifetime, not a durable fact. A clean design records a short-lived lease tied to the authenticated session and workspace grant. Heartbeats extend the lease. Disconnect, expiry, or revoked authorization removes it. After a network break, the client asks to join again and the server evaluates the grant again.

Fail closed.

Keep the terms precise. A channel is a logical delivery and membership boundary; a connection is transport. Many room subscriptions can share one WebSocket or another bidirectional session, so per-room channels do not inherently require per-room network connections. WebRTC data channels are also multiplexed over an underlying peer connection, which is a useful reminder that logical separation and transport fan-out are different design decisions.

## Set the safe membership path

Make one server operation own authorization, subscription, and presence publication. If those steps live in separate handlers, retries can leave a user visible but unable to receive updates, or subscribed before the access check finishes. The operation should be idempotent because reconnects and client retries are normal.

```go
package presence

import (
    "context"
    "errors"
    "time"
)

type JoinRequest struct {
    UserID      string
    WorkspaceID string
    SessionID   string
}

type Authorizer interface {
    CanEnter(context.Context, string, string) (bool, error)
}

type MembershipStore interface {
    UpsertLease(context.Context, JoinRequest, time.Time) error
}

type Subscriber interface {
    Subscribe(context.Context, string, string) error
}

type Service struct {
    Auth  Authorizer
    Store MembershipStore
    Bus   Subscriber
    TTL   time.Duration
}

func (s Service) Join(ctx context.Context, req JoinRequest, now time.Time) error {
    allowed, err := s.Auth.CanEnter(ctx, req.UserID, req.WorkspaceID)
    if err != nil {
        return err // Fail closed when current authorization is unavailable.
    }
    if !allowed {
        return errors.New("workspace access denied")
    }

    // The session key makes a retry refresh the same lease.
    if err := s.Store.UpsertLease(ctx, req, now.Add(s.TTL)); err != nil {
        return err
    }
    return s.Bus.Subscribe(ctx, req.SessionID, req.WorkspaceID)
}
```

In production, the storage and subscription steps need a reconciler because they rarely share a transaction. Give the lease a deterministic key such as `(workspace_id, user_id, session_id)`, then let repeated joins converge. If subscription fails after the lease write, mark the session unhealthy or remove the lease; do not leave a user displayed as online merely because half of the operation succeeded.

Consider the awkward recovery sequence in full. A carrier's laptop loses connectivity while the user is present in `WH-NORTH`; an administrator then revokes that workspace grant; meanwhile, the browser keeps its old room list in memory. When the link returns, a server that trusts the cached list can renew presence before the next chat message reaches its filter. A per-room join handler instead treats `WH-NORTH` as a new request, checks the current grant, denies the subscription, and leaves no lease to advertise. This is why a test that inspects only delivered messages is incomplete. The roster itself is protected data, and its recovery path needs the same authorization decision as message delivery. Record the decision and grant version so the on-call engineer can tell a legitimate denial from a broken subscription without reading chat content.

For a single filtered stream, apply the same rule at more points. The server must evaluate workspace membership before enqueueing each event, before replaying retained events, and again when restoring state after reconnect. Client-side filtering is never an authorization control. The wider shared stream also needs tests showing that a malformed or missing workspace identifier fails closed.

## Run the failure drill before rollout

Test the events that create ambiguous presence, not just a successful join. Start with two workspaces, `WH-NORTH` and `WH-SOUTH`, one dispatcher authorized for both, and one carrier authorized only for north. Then exercise these cases:

1. Revoke the carrier while its client is disconnected; reconnect must not restore north membership.
2. Deliver the same join request twice; the online roster must contain one session lease, not two entries.
3. Stop heartbeats without sending a clean disconnect; the lease must expire and the departure must become visible.
4. Move the dispatcher from north to south during a reconnect loop; no north event or roster snapshot may appear after revocation.
5. Omit the workspace identifier from a publish request; delivery must fail closed rather than fall back to a global audience.

The pass condition is stronger than "no chat message leaked." Capture roster snapshots and delivery records with user, session, workspace, authorization decision, grant version, and reason. Avoid message bodies in routine logs. These fields let an operator distinguish a stale lease from a rejected join without exposing conversation content.

Watch four signals during the rollout: denied rejoins, lease expirations, duplicate active session keys, and the delay between revocation and roster removal. Set alerts from the behavior of your own system rather than borrowing arbitrary thresholds. A spike in denied rejoins may indicate a legitimate policy change; duplicate active keys indicate an idempotency defect. Those demand different responses.

This choice has limits. Per-room channels are a poor fit when every authenticated user always sees the same audience and room membership never affects authorization; a single stream avoids needless subscription bookkeeping there. They also impose more server-side membership records and more churn when users cross many workspaces. The trade-off favors room boundaries only when isolation and presence accuracy justify that operational load. Conversely, one filtering channel can be reasonable when a central policy layer controls every live event, replay, and roster snapshot, and the team can test all three paths as one security boundary.

## Roll forward, roll back, and recover

Deploy the new membership path behind a server-controlled mode flag. Shadow-compute authorization decisions first, comparing the old and new outcomes without changing delivery. Investigate every disagreement involving a revoked grant. Once the results agree, move a small set of internal workspaces to enforced per-room membership and expand only after reconnect and lease-expiry signals remain explainable.

Rollback must preserve denial decisions. If enforcement causes operational trouble, stop new joins and drain affected sessions; do not restore a broad shared subscription that could reintroduce revoked memberships. Clients can show presence as unknown while they retry. Wrong certainty is worse.

Unknown is honest.

During an incident, first disable presence publication for sessions whose authorization cannot be verified. Next, expire their leases, rebuild membership from current grants, and resubscribe. Message delivery and online indicators should recover from the same authoritative workspace membership, or the two views will drift again.

Per-room channels add membership objects and subscription churn. A filtered stream concentrates delivery but multiplies the paths on which filtering must remain correct. For logistics presence, accuracy usually dominates bookkeeping: use logical room boundaries, multiplex them over a manageable transport pool, and make reconnect a fresh authorization event.

## References

- https://www.w3.org/TR/webrtc/
