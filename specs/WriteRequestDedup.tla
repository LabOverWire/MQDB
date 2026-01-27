-------------------------------- MODULE WriteRequestDedup --------------------------------
\* Focused model to check if duplicate WriteRequest messages cause sequence amplification

EXTENDS Integers, FiniteSets

CONSTANTS
    Nodes,
    Primary

VARIABLES
    inbox,
    sequence,
    message_count,
    applied_writes

vars == <<inbox, sequence, message_count, applied_writes>>

Init ==
    /\ inbox = [n \in Nodes |-> <<>>]
    /\ sequence = 0
    /\ message_count = 0
    /\ applied_writes = {}

\* A non-primary node sends a WriteRequest
SendWriteRequest(from, write_id) ==
    /\ from # Primary
    /\ inbox' = [inbox EXCEPT ![Primary] = Append(@, [type |-> "WriteRequest",
                                                       from |-> from,
                                                       write_id |-> write_id])]
    /\ message_count' = message_count + 1
    /\ UNCHANGED <<sequence, applied_writes>>

\* Primary handles WriteRequest - THE BUG: no deduplication!
HandleWriteRequest ==
    /\ Len(inbox[Primary]) > 0
    /\ LET msg == Head(inbox[Primary])
       IN /\ msg.type = "WriteRequest"
          \* BUG: sequence advances for EVERY WriteRequest, even duplicates
          /\ sequence' = sequence + 1
          /\ applied_writes' = applied_writes \cup {<<msg.write_id, sequence'>>}
          /\ inbox' = [inbox EXCEPT ![Primary] = Tail(@)]
          /\ message_count' = message_count + 1

\* Network can duplicate messages (QUIC guarantees delivery but messages
\* could be resent if inbox was full and sender retries)
DuplicateMessage(n) ==
    /\ Len(inbox[n]) > 0
    /\ LET msg == Head(inbox[n])
       IN inbox' = [inbox EXCEPT ![n] = Append(@, msg)]
    /\ message_count' = message_count + 1
    /\ UNCHANGED <<sequence, applied_writes>>

Next ==
    \/ \E from \in Nodes, wid \in 1..3: SendWriteRequest(from, wid)
    \/ HandleWriteRequest
    \/ \E n \in Nodes: DuplicateMessage(n)

Spec == Init /\ [][Next]_vars

\* INVARIANT: Same write_id should only produce ONE sequence number
\* If violated, we have amplification
NoSequenceAmplification ==
    \A wid \in 1..3:
        LET seqs == {s : <<w, s>> \in applied_writes : w = wid}
        IN Cardinality(seqs) <= 1

\* Message count should be bounded relative to unique writes
BoundedMessages ==
    message_count < 50

TypeOK ==
    /\ sequence \in Nat
    /\ message_count \in Nat

=============================================================================
