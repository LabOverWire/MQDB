---- MODULE RaftFlappingBug ----
EXTENDS Naturals, FiniteSets

CONSTANTS Nodes, MaxTerm

VARIABLES
    state,
    term,
    votedFor,
    votes,
    peers

vars == <<state, term, votedFor, votes, peers>>

Follower == "Follower"
Candidate == "Candidate"
Leader == "Leader"
None == "None"

QuorumSize(n) == (Cardinality(peers[n]) \div 2) + 1

HasQuorum(n) == Cardinality(votes[n]) >= QuorumSize(n)

Init ==
    /\ state = [n \in Nodes |-> Follower]
    /\ term = [n \in Nodes |-> 0]
    /\ votedFor = [n \in Nodes |-> None]
    /\ votes = [n \in Nodes |-> {}]
    /\ peers = [n \in Nodes |-> {}]

AddPeer(n, p) ==
    /\ n # p
    /\ p \notin peers[n]
    /\ peers' = [peers EXCEPT ![n] = peers[n] \union {p}]
    /\ UNCHANGED <<state, term, votedFor, votes>>

Timeout(n) ==
    /\ state[n] \in {Follower, Candidate}
    /\ term[n] < MaxTerm
    /\ state' = [state EXCEPT ![n] = Candidate]
    /\ term' = [term EXCEPT ![n] = term[n] + 1]
    /\ votedFor' = [votedFor EXCEPT ![n] = n]
    /\ votes' = [votes EXCEPT ![n] = {n}]
    /\ UNCHANGED peers

BecomeLeader(n) ==
    /\ state[n] = Candidate
    /\ HasQuorum(n)
    /\ state' = [state EXCEPT ![n] = Leader]
    /\ UNCHANGED <<term, votedFor, votes, peers>>

RequestVote(candidate, voter) ==
    /\ state[candidate] = Candidate
    /\ candidate # voter
    /\ voter \in peers[candidate]
    /\ term[candidate] >= term[voter]
    /\ votedFor[voter] \in {None, candidate}
    /\ votedFor' = [votedFor EXCEPT ![voter] = candidate]
    /\ votes' = [votes EXCEPT ![candidate] = votes[candidate] \union {voter}]
    /\ IF term[candidate] > term[voter]
       THEN /\ term' = [term EXCEPT ![voter] = term[candidate]]
            /\ state' = [state EXCEPT ![voter] = Follower]
       ELSE UNCHANGED <<term, state>>
    /\ UNCHANGED peers

ReceiveHigherTerm(n) ==
    /\ state[n] = Leader
    /\ \E other \in Nodes:
        /\ other # n
        /\ term[other] > term[n]
    /\ state' = [state EXCEPT ![n] = Follower]
    /\ term' = [term EXCEPT ![n] = term[n]]
    /\ votedFor' = [votedFor EXCEPT ![n] = None]
    /\ votes' = [votes EXCEPT ![n] = {}]
    /\ UNCHANGED peers

Next ==
    \/ \E n \in Nodes: \E p \in Nodes: AddPeer(n, p)
    \/ \E n \in Nodes: Timeout(n)
    \/ \E n \in Nodes: BecomeLeader(n)
    \/ \E c, v \in Nodes: RequestVote(c, v)
    \/ \E n \in Nodes: ReceiveHigherTerm(n)

InvAtMostOneLeaderPerTerm ==
    \A n1 \in Nodes: \A n2 \in Nodes:
        (state[n1] = Leader /\ state[n2] = Leader /\ n1 # n2)
        => term[n1] # term[n2]

InvLeaderHasQuorum ==
    \A n \in Nodes:
        state[n] = Leader => HasQuorum(n)

InvNoPeersLeaderWithOnlyOwnVote ==
    \A n \in Nodes:
        (state[n] = Leader /\ peers[n] = {})
        => votes[n] = {n}

InvSplitBrain ==
    Cardinality({n \in Nodes: state[n] = Leader}) <= 1

====
