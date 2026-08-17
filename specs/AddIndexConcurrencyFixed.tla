---- MODULE AddIndexConcurrencyFixed ----
\* Companion to AddIndexConcurrency: the same model instantiated with
\* Serialize <- TRUE (the admin-mutex fix). Kept as its own module so base-name
\* .cfg discovery loads AddIndexConcurrencyFixed.cfg and checks the fixed case
\* (expected: no violation), while AddIndexConcurrency.cfg checks the current-code
\* case (expected: Consistent violated).
VARIABLES mem, disk, mutex, pc, snap, mrg

Base == INSTANCE AddIndexConcurrency WITH Serialize <- TRUE

vars == <<mem, disk, mutex, pc, snap, mrg>>
Init == Base!Init
Next == Base!Next
SpecFixed == Init /\ [][Next]_vars
ConsistentFixed == Base!Consistent
====
