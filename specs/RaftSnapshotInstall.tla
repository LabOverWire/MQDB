------------------------ MODULE RaftSnapshotInstall ------------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Nodes, MaxTerm, MaxClient, MaxRestart, Fix, Candidates, Restartable

None == 0

VARIABLES term, role, votedFor, votes,
          base, baseTerm, log, commitIndex, sm,
          pBase, pBaseTerm, pLog, pSm,
          nextIndex, matchIndex, msgs, clientCount, restarts, installs

vars == <<term, role, votedFor, votes, base, baseTerm, log, commitIndex, sm,
          pBase, pBaseTerm, pLog, pSm, nextIndex, matchIndex, msgs,
          clientCount, restarts, installs>>

Min(a, b) == IF a < b THEN a ELSE b
Peers(n) == Nodes \ {n}
Quorum == Cardinality(Nodes) \div 2 + 1

LastIdx(n) == base[n] + Len(log[n])
LastTerm(n) == IF Len(log[n]) = 0 THEN baseTerm[n] ELSE log[n][Len(log[n])]
Known(n, i) == i = 0 \/ (i >= base[n] /\ i <= LastIdx(n))
TermAt(n, i) == IF i = 0 THEN 0
                ELSE IF i = base[n] THEN baseTerm[n]
                ELSE log[n][i - base[n]]
Applied(n) == Len(sm[n])

Leader0 == CHOOSE n \in Nodes : \A m \in Nodes : n <= m

Init ==
    /\ term = [n \in Nodes |-> 1]
    /\ role = [n \in Nodes |-> IF n = Leader0 THEN "L" ELSE "F"]
    /\ votedFor = [n \in Nodes |-> Leader0]
    /\ votes = [n \in Nodes |-> {}]
    /\ base = [n \in Nodes |-> 0]
    /\ baseTerm = [n \in Nodes |-> 0]
    /\ log = [n \in Nodes |-> IF n = Leader0 THEN <<1>> ELSE <<>>]
    /\ commitIndex = [n \in Nodes |-> 0]
    /\ sm = [n \in Nodes |-> <<>>]
    /\ pBase = [n \in Nodes |-> 0]
    /\ pBaseTerm = [n \in Nodes |-> 0]
    /\ pLog = [n \in Nodes |-> IF n = Leader0 THEN <<1>> ELSE <<>>]
    /\ pSm = [n \in Nodes |-> <<>>]
    /\ nextIndex = [n \in Nodes |-> [m \in Nodes |-> 2]]
    /\ matchIndex = [n \in Nodes |-> [m \in Nodes |-> 0]]
    /\ msgs = {}
    /\ clientCount = 0
    /\ restarts = 0
    /\ installs = 0

ApplyTo(n, nb, nbt, nl, c) ==
    LET ta(i) == IF i = nb THEN nbt ELSE nl[i - nb]
        cur == sm[n]
    IN IF c <= Len(cur) THEN cur
       ELSE cur \o [j \in 1..(c - Len(cur)) |-> ta(Len(cur) + j)]

DurableLog(n, newLog) ==
    /\ pLog' = [pLog EXCEPT ![n] =
          SubSeq(@, 1, base[n] - pBase[n]) \o newLog]

Timeout(n) ==
    /\ n \in Candidates
    /\ role[n] # "L"
    /\ term[n] < MaxTerm
    /\ term' = [term EXCEPT ![n] = @ + 1]
    /\ role' = [role EXCEPT ![n] = "C"]
    /\ votedFor' = [votedFor EXCEPT ![n] = n]
    /\ votes' = [votes EXCEPT ![n] = {n}]
    /\ msgs' = msgs \cup
         {[type |-> "RV", from |-> n, to |-> p, term |-> term[n] + 1,
           lastIdx |-> LastIdx(n), lastTerm |-> LastTerm(n)] : p \in Peers(n)}
    /\ UNCHANGED <<base, baseTerm, log, commitIndex, sm, pBase, pBaseTerm, pLog,
                   pSm, nextIndex, matchIndex, clientCount, restarts, installs>>

HandleRV(m) ==
    /\ m \in msgs /\ m.type = "RV"
    /\ LET r == m.to
           higher == m.term > term[r]
           t1 == IF higher THEN m.term ELSE term[r]
           vf1 == IF higher THEN None ELSE votedFor[r]
           logOk == m.lastTerm > LastTerm(r)
                    \/ (m.lastTerm = LastTerm(r) /\ m.lastIdx >= LastIdx(r))
           grant == m.term >= t1 /\ (vf1 = None \/ vf1 = m.from) /\ logOk
       IN
       /\ term' = [term EXCEPT ![r] = t1]
       /\ role' = [role EXCEPT ![r] = IF higher THEN "F" ELSE @]
       /\ votes' = [votes EXCEPT ![r] = IF higher THEN {} ELSE @]
       /\ votedFor' = [votedFor EXCEPT ![r] = IF grant THEN m.from ELSE vf1]
       /\ msgs' = (msgs \ {m}) \cup
            {[type |-> "RVR", from |-> r, to |-> m.from, term |-> t1, granted |-> grant]}
    /\ UNCHANGED <<base, baseTerm, log, commitIndex, sm, pBase, pBaseTerm, pLog,
                   pSm, nextIndex, matchIndex, clientCount, restarts, installs>>

BecomeLeader(c) ==
    /\ role' = [role EXCEPT ![c] = "L"]
    /\ log' = [log EXCEPT ![c] = Append(@, term[c])]
    /\ pLog' = [pLog EXCEPT ![c] = Append(@, term[c])]
    /\ nextIndex' = [nextIndex EXCEPT ![c] = [m \in Nodes |-> LastIdx(c) + 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![c] = [m \in Nodes |-> 0]]

HandleRVR(m) ==
    /\ m \in msgs /\ m.type = "RVR"
    /\ LET c == m.to IN
       IF m.term > term[c]
         THEN /\ term' = [term EXCEPT ![c] = m.term]
              /\ role' = [role EXCEPT ![c] = "F"]
              /\ votedFor' = [votedFor EXCEPT ![c] = None]
              /\ votes' = [votes EXCEPT ![c] = {}]
              /\ msgs' = msgs \ {m}
              /\ UNCHANGED <<log, pLog, nextIndex, matchIndex>>
         ELSE IF role[c] = "C" /\ m.term = term[c] /\ m.granted
           THEN LET v == votes[c] \cup {m.from} IN
                /\ votes' = [votes EXCEPT ![c] = v]
                /\ msgs' = msgs \ {m}
                /\ IF Cardinality(v) >= Quorum
                     THEN BecomeLeader(c)
                     ELSE UNCHANGED <<role, log, pLog, nextIndex, matchIndex>>
                /\ UNCHANGED <<term, votedFor>>
           ELSE /\ msgs' = msgs \ {m}
                /\ UNCHANGED <<term, role, votedFor, votes, log, pLog, nextIndex, matchIndex>>
    /\ UNCHANGED <<base, baseTerm, commitIndex, sm, pBase, pBaseTerm, pSm,
                   clientCount, restarts, installs>>

ClientRequest(l) ==
    /\ role[l] = "L"
    /\ clientCount < MaxClient
    /\ log' = [log EXCEPT ![l] = Append(@, term[l])]
    /\ pLog' = [pLog EXCEPT ![l] = Append(@, term[l])]
    /\ clientCount' = clientCount + 1
    /\ UNCHANGED <<term, role, votedFor, votes, base, baseTerm, commitIndex, sm,
                   pBase, pBaseTerm, pSm, nextIndex, matchIndex, msgs, restarts, installs>>

Entries(n, from) == IF from > LastIdx(n) THEN <<>>
                    ELSE SubSeq(log[n], from - base[n], Len(log[n]))

SendAE(l, p) ==
    /\ role[l] = "L"
    /\ p \in Peers(l)
    /\ LET next == nextIndex[l][p]
           m == IF Fix /\ next <= base[l]
                  THEN [type |-> "IS", from |-> l, to |-> p, term |-> term[l],
                        lastIdx |-> Applied(l), lastTerm |-> TermAt(l, Applied(l)),
                        sm |-> sm[l]]
                  ELSE IF next - 1 >= base[l]
                    THEN [type |-> "AE", from |-> l, to |-> p, term |-> term[l],
                          prev |-> next - 1, prevTerm |-> TermAt(l, next - 1),
                          entries |-> Entries(l, next), first |-> next,
                          commit |-> commitIndex[l]]
                    ELSE [type |-> "AE", from |-> l, to |-> p, term |-> term[l],
                          prev |-> next - 1, prevTerm |-> 0,
                          entries |-> log[l], first |-> base[l] + 1,
                          commit |-> commitIndex[l]]
       IN /\ m \notin msgs
          /\ msgs' = msgs \cup {m}
    /\ UNCHANGED <<term, role, votedFor, votes, base, baseTerm, log, commitIndex, sm,
                   pBase, pBaseTerm, pLog, pSm, nextIndex, matchIndex, clientCount,
                   restarts, installs>>

Overlay(n, first0, es0) ==
    LET skip == IF first0 <= base[n] THEN base[n] - first0 + 1 ELSE 0
        first == first0 + skip
        es == IF skip >= Len(es0) THEN <<>> ELSE SubSeq(es0, skip + 1, Len(es0))
        idxs == {first + j - 1 : j \in 1..Len(es)}
        rel(i) == i - base[n]
        conflict == {i \in idxs : i <= LastIdx(n) /\ log[n][rel(i)] # es[i - first + 1]}
    IN IF conflict # {}
         THEN LET k == CHOOSE i \in conflict : \A j \in conflict : i <= j
              IN SubSeq(log[n], 1, rel(k) - 1) \o SubSeq(es, k - first + 1, Len(es))
         ELSE IF first + Len(es) - 1 <= LastIdx(n) THEN log[n]
         ELSE IF first <= LastIdx(n) + 1
           THEN SubSeq(log[n], 1, first - 1 - base[n]) \o es
         ELSE log[n]

StepDownVars(r, t) ==
    /\ term' = [term EXCEPT ![r] = t]
    /\ role' = [role EXCEPT ![r] = "F"]
    /\ votedFor' = [votedFor EXCEPT ![r] = IF t > term[r] THEN None ELSE @]
    /\ votes' = [votes EXCEPT ![r] = IF t > term[r] \/ role[r] # "F" THEN {} ELSE @]

Reply(m, r, t, ok, mi) ==
    [type |-> "AER", from |-> r, to |-> m.from, term |-> t, success |-> ok, match |-> mi]

HandleAE(m) ==
    /\ m \in msgs /\ m.type = "AE"
    /\ LET r == m.to IN
       IF m.term < term[r]
         THEN /\ msgs' = (msgs \ {m}) \cup {Reply(m, r, term[r], FALSE, 0)}
              /\ UNCHANGED <<term, role, votedFor, votes, log, pLog, commitIndex, sm>>
         ELSE IF Fix
           THEN LET skip == IF m.prev < base[r] THEN base[r] - m.prev ELSE 0
                    first == m.first + skip
                    es == IF skip >= Len(m.entries) THEN <<>>
                          ELSE SubSeq(m.entries, skip + 1, Len(m.entries))
                    prev == first - 1
                    ok == prev <= LastIdx(r) /\ prev >= base[r]
                          /\ (prev = 0 \/ TermAt(r, prev) = (IF skip > 0 THEN baseTerm[r] ELSE m.prevTerm))
                    newLog == IF ok THEN Overlay(r, first, es) ELSE log[r]
                    verified == IF prev + Len(es) > base[r] THEN prev + Len(es) ELSE base[r]
                    newCommit == IF ok /\ m.commit > commitIndex[r]
                                   THEN Min(m.commit, verified) ELSE commitIndex[r]
                IN /\ StepDownVars(r, m.term)
                   /\ log' = [log EXCEPT ![r] = newLog]
                   /\ IF ok THEN DurableLog(r, newLog) ELSE UNCHANGED pLog
                   /\ commitIndex' = [commitIndex EXCEPT ![r] = newCommit]
                   /\ sm' = [sm EXCEPT ![r] = ApplyTo(r, base[r], baseTerm[r], newLog, newCommit)]
                   /\ msgs' = (msgs \ {m}) \cup
                        {Reply(m, r, m.term, ok, IF ok THEN verified ELSE 0)}
           ELSE LET ok == m.prev = 0 \/ (Known(r, m.prev) /\ TermAt(r, m.prev) = m.prevTerm)
                    newLog == IF ok THEN Overlay(r, m.first, m.entries) ELSE log[r]
                    newLast == base[r] + Len(newLog)
                    newCommit == IF ok /\ m.commit > commitIndex[r]
                                   THEN Min(m.commit, newLast) ELSE commitIndex[r]
                IN /\ StepDownVars(r, m.term)
                   /\ log' = [log EXCEPT ![r] = newLog]
                   /\ IF ok THEN DurableLog(r, newLog) ELSE UNCHANGED pLog
                   /\ commitIndex' = [commitIndex EXCEPT ![r] = newCommit]
                   /\ sm' = [sm EXCEPT ![r] = ApplyTo(r, base[r], baseTerm[r], newLog, newCommit)]
                   /\ msgs' = (msgs \ {m}) \cup
                        {Reply(m, r, m.term, ok, IF ok THEN newLast ELSE 0)}
    /\ UNCHANGED <<base, baseTerm, pBase, pBaseTerm, pSm, nextIndex, matchIndex,
                   clientCount, restarts, installs>>

HandleIS(m) ==
    /\ Fix
    /\ m \in msgs /\ m.type = "IS"
    /\ LET r == m.to IN
       IF m.term < term[r]
         THEN /\ msgs' = (msgs \ {m}) \cup {Reply(m, r, term[r], FALSE, 0)}
              /\ UNCHANGED <<term, role, votedFor, votes, base, baseTerm, log, commitIndex,
                             sm, pBase, pBaseTerm, pLog, pSm, installs>>
         ELSE IF m.lastIdx <= commitIndex[r]
           THEN /\ StepDownVars(r, m.term)
                /\ msgs' = (msgs \ {m}) \cup {Reply(m, r, m.term, TRUE, commitIndex[r])}
                /\ UNCHANGED <<base, baseTerm, log, commitIndex, sm, pBase, pBaseTerm,
                               pLog, pSm, installs>>
           ELSE LET keep == m.lastIdx <= LastIdx(r) /\ m.lastIdx >= base[r]
                            /\ TermAt(r, m.lastIdx) = m.lastTerm
                    suffix == IF keep THEN SubSeq(log[r], m.lastIdx - base[r] + 1, Len(log[r]))
                              ELSE <<>>
                IN
                /\ StepDownVars(r, m.term)
                /\ base' = [base EXCEPT ![r] = m.lastIdx]
                /\ baseTerm' = [baseTerm EXCEPT ![r] = m.lastTerm]
                /\ log' = [log EXCEPT ![r] = suffix]
                /\ commitIndex' = [commitIndex EXCEPT ![r] = m.lastIdx]
                /\ sm' = [sm EXCEPT ![r] = m.sm]
                /\ pBase' = [pBase EXCEPT ![r] = m.lastIdx]
                /\ pBaseTerm' = [pBaseTerm EXCEPT ![r] = m.lastTerm]
                /\ pLog' = [pLog EXCEPT ![r] = suffix]
                /\ pSm' = [pSm EXCEPT ![r] = m.sm]
                /\ installs' = installs + 1
                /\ msgs' = (msgs \ {m}) \cup {Reply(m, r, m.term, TRUE, m.lastIdx)}
    /\ UNCHANGED <<nextIndex, matchIndex, clientCount, restarts>>

CommitTo(l, mi) ==
    LET cands == {n \in (base[l] + 1)..LastIdx(l) :
                    /\ n > commitIndex[l]
                    /\ TermAt(l, n) = term[l]
                    /\ Cardinality({p \in Peers(l) : mi[p] >= n}) + 1 >= Quorum}
    IN IF cands = {} THEN commitIndex[l] ELSE CHOOSE n \in cands : \A k \in cands : n >= k

HandleAER(m) ==
    /\ m \in msgs /\ m.type = "AER"
    /\ LET l == m.to IN
       IF m.term > term[l]
         THEN /\ term' = [term EXCEPT ![l] = m.term]
              /\ role' = [role EXCEPT ![l] = "F"]
              /\ votedFor' = [votedFor EXCEPT ![l] = None]
              /\ votes' = [votes EXCEPT ![l] = {}]
              /\ msgs' = msgs \ {m}
              /\ UNCHANGED <<nextIndex, matchIndex, commitIndex, sm>>
         ELSE IF role[l] # "L" \/ m.term < term[l]
           THEN /\ msgs' = msgs \ {m}
                /\ UNCHANGED <<term, role, votedFor, votes, nextIndex, matchIndex, commitIndex, sm>>
           ELSE IF Fix /\ m.success /\ m.match < matchIndex[l][m.from]
             THEN /\ msgs' = msgs \ {m}
                  /\ UNCHANGED <<term, role, votedFor, votes, nextIndex, matchIndex, commitIndex, sm>>
           ELSE IF m.success
             THEN LET mi == [matchIndex[l] EXCEPT ![m.from] = m.match]
                      c == CommitTo(l, mi)
                  IN /\ matchIndex' = [matchIndex EXCEPT ![l] = mi]
                     /\ nextIndex' = [nextIndex EXCEPT ![l][m.from] = m.match + 1]
                     /\ commitIndex' = [commitIndex EXCEPT ![l] = c]
                     /\ sm' = [sm EXCEPT ![l] = ApplyTo(l, base[l], baseTerm[l], log[l], c)]
                     /\ msgs' = msgs \ {m}
                     /\ UNCHANGED <<term, role, votedFor, votes>>
             ELSE /\ nextIndex' = [nextIndex EXCEPT ![l][m.from] = m.match + 1]
                  /\ msgs' = msgs \ {m}
                  /\ UNCHANGED <<term, role, votedFor, votes, matchIndex, commitIndex, sm>>
    /\ UNCHANGED <<base, baseTerm, log, pBase, pBaseTerm, pLog, pSm,
                   clientCount, restarts, installs>>

Compact(n) ==
    /\ Applied(n) > base[n]
    /\ LET k == Applied(n) IN
         /\ base' = [base EXCEPT ![n] = k]
         /\ baseTerm' = [baseTerm EXCEPT ![n] = TermAt(n, k)]
         /\ log' = [log EXCEPT ![n] = SubSeq(@, k - base[n] + 1, Len(@))]
    /\ UNCHANGED <<term, role, votedFor, votes, commitIndex, sm, pBase, pBaseTerm,
                   pLog, pSm, nextIndex, matchIndex, msgs, clientCount, restarts, installs>>

Restart(n) ==
    /\ n \in Restartable
    /\ restarts < MaxRestart
    /\ restarts' = restarts + 1
    /\ role' = [role EXCEPT ![n] = "F"]
    /\ votes' = [votes EXCEPT ![n] = {}]
    /\ base' = [base EXCEPT ![n] = pBase[n]]
    /\ baseTerm' = [baseTerm EXCEPT ![n] = pBaseTerm[n]]
    /\ log' = [log EXCEPT ![n] = pLog[n]]
    /\ commitIndex' = [commitIndex EXCEPT ![n] = pBase[n]]
    /\ sm' = [sm EXCEPT ![n] = pSm[n]]
    /\ nextIndex' = [nextIndex EXCEPT ![n] = [m \in Nodes |-> 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![n] = [m \in Nodes |-> 0]]
    /\ UNCHANGED <<term, votedFor, pBase, pBaseTerm, pLog, pSm, msgs, clientCount, installs>>

DropMsg(m) ==
    /\ m \in msgs
    /\ msgs' = msgs \ {m}
    /\ UNCHANGED <<term, role, votedFor, votes, base, baseTerm, log, commitIndex, sm,
                   pBase, pBaseTerm, pLog, pSm, nextIndex, matchIndex, clientCount,
                   restarts, installs>>

Next ==
    \/ \E n \in Nodes : Timeout(n) \/ ClientRequest(n) \/ Compact(n) \/ Restart(n)
    \/ \E l, p \in Nodes : SendAE(l, p)
    \/ \E m \in msgs : HandleRV(m) \/ HandleRVR(m) \/ HandleAE(m) \/ HandleIS(m)
                       \/ HandleAER(m) \/ DropMsg(m)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
SeqPrefix(x, y) == Len(x) <= Len(y) /\ \A i \in 1..Len(x) : x[i] = y[i]

InvStateMachineSafety ==
    \A a, b \in Nodes : SeqPrefix(sm[a], sm[b]) \/ SeqPrefix(sm[b], sm[a])

InvDurableStateConsistent ==
    \A a, b \in Nodes : SeqPrefix(pSm[a], sm[b]) \/ SeqPrefix(sm[b], pSm[a])

InvNeverInstalled == installs = 0

MsgBound == Cardinality(msgs) <= 4
MsgBound3 == Cardinality(msgs) <= 3
=============================================================================
