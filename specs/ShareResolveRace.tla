---- MODULE ShareResolveRace ----
(***************************************************************************)
(* Cluster async email-share resolution (#122) racing with a concurrent    *)
(* unshare / delete of the same (resource, grantee).                        *)
(*                                                                          *)
(* Code mapping (crates/mqdb-cluster/src/cluster):                          *)
(*   ShareBegin(i)    = db_handler/json_ops.rs route_share_op ->            *)
(*                      resolve_share_identity (Share, remote-miss) ->      *)
(*                      node_controller/sharing.rs begin_identity_scatter:  *)
(*                      registers a PendingScatterRequest continuation and  *)
(*                      returns Suspended; NO grant is written yet.         *)
(*   ResolveComplete  = node_controller/query.rs handle_scatter_list_       *)
(*                      response -> sharing.rs complete_share_resolution ->  *)
(*                      handle_share_local: returns 404 if the resource is   *)
(*                      gone (db_get is_none), else write_grant.            *)
(*   Unshare          = route_share_op (Unshare) -> handle_unshare_local -> *)
(*                      clear_grant. Synchronous; does NOT cancel any        *)
(*                      in-flight resolve continuation.                     *)
(*   Delete           = handle_json_delete -> clear_all_resource_grants and *)
(*                      the resource record removed.                        *)
(*                                                                          *)
(* revoked tracks "the owner's most recent of {share, unshare} was an       *)
(* unshare" -- i.e. the owner's standing intent is that the grantee has no  *)
(* access. InvNoResurrection asserts a completing resolve never grants      *)
(* access against that standing revoke.                                    *)
(*                                                                          *)
(* Guarded = FALSE is the pre-guard code. Guarded = TRUE models the fix,    *)
(* implemented as sharing.rs invalidate_share_resolutions (an unshare marks  *)
(* the in-flight continuation for the same (entity,id,grantee_key)) plus the *)
(* complete_share_resolution early-return that skips an invalidated write.   *)
(* The model has a single locus; the implementation enforces on the node     *)
(* holding the continuation, which is where a single owner's share and       *)
(* unshare both arrive. Share/unshare split across two nodes within the      *)
(* resolution window is a documented narrower residual.                     *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS Guarded, MaxShares, MaxUnshares

VARIABLES exists, grant, revoked, pending, unshares

vars == <<exists, grant, revoked, pending, unshares>>

Ids == 1..MaxShares

TypeOK ==
    /\ exists \in BOOLEAN
    /\ grant \in BOOLEAN
    /\ revoked \in BOOLEAN
    /\ pending \in [Ids -> {"none", "live", "dead"}]
    /\ unshares \in 0..MaxUnshares

Init ==
    /\ exists = TRUE
    /\ grant = FALSE
    /\ revoked = FALSE
    /\ pending = [i \in Ids |-> "none"]
    /\ unshares = 0

\* A share to an email whose identity link is on another partition suspends
\* on the scatter: continuation registered, no grant written. A new share
\* supersedes any standing revoke intent.
ShareBegin(i) ==
    /\ pending[i] = "none"
    /\ pending' = [pending EXCEPT ![i] = "live"]
    /\ revoked' = FALSE
    /\ UNCHANGED <<exists, grant, unshares>>

\* Synchronous revoke: clears the grant. It does not touch pending resolves
\* in the code; here we additionally flip live continuations to "dead" so the
\* Guarded variant can express the fix (an unshare after suspend).
Unshare ==
    /\ exists = TRUE
    /\ unshares < MaxUnshares
    /\ grant' = FALSE
    /\ revoked' = TRUE
    /\ unshares' = unshares + 1
    /\ pending' = [i \in Ids |-> IF pending[i] = "live" THEN "dead" ELSE pending[i]]
    /\ UNCHANGED exists

\* The scatter completes and the grant is written on the resource primary.
\* 404 when the resource is gone (delete-safety). Guarded skips a write whose
\* continuation was invalidated by an intervening unshare.
ResolveComplete(i) ==
    /\ pending[i] \in {"live", "dead"}
    /\ pending' = [pending EXCEPT ![i] = "none"]
    /\ grant' = IF exists /\ (~Guarded \/ pending[i] = "live")
                THEN TRUE
                ELSE grant
    /\ UNCHANGED <<exists, revoked, unshares>>

\* Resource deleted: grants cleared and the record removed. A later resolve
\* 404s (see ResolveComplete's exists guard).
Delete ==
    /\ exists = TRUE
    /\ exists' = FALSE
    /\ grant' = FALSE
    /\ UNCHANGED <<revoked, pending, unshares>>

Next ==
    \/ \E i \in Ids : ShareBegin(i)
    \/ Unshare
    \/ \E i \in Ids : ResolveComplete(i)
    \/ Delete

Spec == Init /\ [][Next]_vars

InvNoResurrection == revoked => ~grant
====
