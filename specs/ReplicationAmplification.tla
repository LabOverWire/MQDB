-------------------------------- MODULE ReplicationAmplification --------------------------------
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS Nodes, Partitions

VARIABLES
    inbox,
    processed_writes,
    message_count

vars == <<inbox, processed_writes, message_count>>

Init ==
    /\ inbox = [n \in Nodes |-> <<>>]
    /\ processed_writes = [n \in Nodes |-> {}]
    /\ message_count = 0

TypeOK ==
    /\ inbox \in [Nodes -> Seq([type: {"Write", "WriteRequest", "Ack"},
                                 partition: Partitions,
                                 from: Nodes,
                                 is_broadcast: BOOLEAN,
                                 seq: Nat])]
    /\ processed_writes \in [Nodes -> SUBSET (Partitions \times Nat)]
    /\ message_count \in Nat

Primary(p) ==
    CHOOSE n \in Nodes : TRUE

Replicas(p) ==
    Nodes \ {Primary(p)}

Send(to, msg) ==
    inbox' = [inbox EXCEPT ![to] = Append(@, msg)]

ClientWrite(n, p, seq, is_broadcast) ==
    /\ message_count' = message_count + 1
    /\ IF is_broadcast
       THEN
         /\ processed_writes' = [processed_writes EXCEPT ![n] = @ \cup {<<p, seq>>}]
         /\ inbox' = [m \in Nodes |->
                        IF m # n
                        THEN Append(inbox[m], [type |-> "WriteRequest",
                                               partition |-> p,
                                               from |-> n,
                                               is_broadcast |-> TRUE,
                                               seq |-> seq])
                        ELSE inbox[m]]
       ELSE IF n = Primary(p)
            THEN
              /\ processed_writes' = [processed_writes EXCEPT ![n] = @ \cup {<<p, seq>>}]
              /\ inbox' = [m \in Nodes |->
                            IF m \in Replicas(p)
                            THEN Append(inbox[m], [type |-> "Write",
                                                   partition |-> p,
                                                   from |-> n,
                                                   is_broadcast |-> FALSE,
                                                   seq |-> seq])
                            ELSE inbox[m]]
            ELSE
              /\ UNCHANGED processed_writes
              /\ Send(Primary(p), [type |-> "WriteRequest",
                                   partition |-> p,
                                   from |-> n,
                                   is_broadcast |-> FALSE,
                                   seq |-> seq])

HandleWriteRequest(n) ==
    /\ Len(inbox[n]) > 0
    /\ LET msg == Head(inbox[n])
       IN /\ msg.type = "WriteRequest"
          /\ inbox' = [inbox EXCEPT ![n] = Tail(@)]
          /\ IF msg.is_broadcast
             THEN
               /\ IF <<msg.partition, msg.seq>> \notin processed_writes[n]
                  THEN processed_writes' = [processed_writes EXCEPT ![n] = @ \cup {<<msg.partition, msg.seq>>}]
                  ELSE UNCHANGED processed_writes
               /\ message_count' = message_count + 1
             ELSE IF n = Primary(msg.partition)
                  THEN
                    /\ processed_writes' = [processed_writes EXCEPT ![n] = @ \cup {<<msg.partition, msg.seq>>}]
                    /\ message_count' = message_count + Cardinality(Replicas(msg.partition))
                  ELSE
                    /\ UNCHANGED processed_writes
                    /\ UNCHANGED message_count

HandleWrite(n) ==
    /\ Len(inbox[n]) > 0
    /\ LET msg == Head(inbox[n])
       IN /\ msg.type = "Write"
          /\ inbox' = [inbox EXCEPT ![n] = Tail(@)]
          /\ processed_writes' = [processed_writes EXCEPT ![n] = @ \cup {<<msg.partition, msg.seq>>}]
          /\ message_count' = message_count + 1

Next ==
    \/ \E n \in Nodes, p \in Partitions, seq \in 1..3, is_b \in BOOLEAN:
         ClientWrite(n, p, seq, is_b)
    \/ \E n \in Nodes: HandleWriteRequest(n)
    \/ \E n \in Nodes: HandleWrite(n)

Spec == Init /\ [][Next]_vars

NoAmplification ==
    message_count < 100

Fairness ==
    /\ \A n \in Nodes: WF_vars(HandleWriteRequest(n))
    /\ \A n \in Nodes: WF_vars(HandleWrite(n))

=============================================================================
