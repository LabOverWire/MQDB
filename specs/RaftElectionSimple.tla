---- MODULE RaftElectionSimple ----
EXTENDS Naturals, FiniteSets

CONSTANTS Nodes, MaxTerm

VARIABLES
    state,
    term,
    votedFor,
    votes,
    leader

vars == <<state, term, votedFor, votes, leader>>

Follower == "Follower"
Candidate == "Candidate"
Leader == "Leader"
None == "None"

QuorumSize == (Cardinality(Nodes) \div 2) + 1

HasQuorum(n) == Cardinality(votes[n]) >= QuorumSize

Init ==
    /\ state = [n \in Nodes |-> Follower]
    /\ term = [n \in Nodes |-> 0]
    /\ votedFor = [n \in Nodes |-> None]
    /\ votes = [n \in Nodes |-> {}]
    /\ leader = [n \in Nodes |-> None]

Timeout(n) ==
    /\ state[n] \in {Follower, Candidate}
    /\ term[n] < MaxTerm
    /\ state' = [state EXCEPT ![n] = Candidate]
    /\ term' = [term EXCEPT ![n] = term[n] + 1]
    /\ votedFor' = [votedFor EXCEPT ![n] = n]
    /\ votes' = [votes EXCEPT ![n] = {n}]
    /\ leader' = [leader EXCEPT ![n] = None]

RequestVote(candidate, voter) ==
    /\ state[candidate] = Candidate
    /\ candidate # voter
    /\ term[candidate] >= term[voter]
    /\ votedFor[voter] \in {None, candidate}
    /\ votedFor' = [votedFor EXCEPT ![voter] = candidate]
    /\ votes' = [votes EXCEPT ![candidate] = votes[candidate] \union {voter}]
    /\ IF term[candidate] > term[voter]
       THEN /\ term' = [term EXCEPT ![voter] = term[candidate]]
            /\ state' = [state EXCEPT ![voter] = Follower]
            /\ leader' = [leader EXCEPT ![voter] = None]
       ELSE /\ UNCHANGED <<term, state, leader>>

BecomeLeader(n) ==
    /\ state[n] = Candidate
    /\ HasQuorum(n)
    /\ state' = [state EXCEPT ![n] = Leader]
    /\ leader' = [leader EXCEPT ![n] = n]
    /\ UNCHANGED <<term, votedFor, votes>>

Heartbeat(leaderNode, follower) ==
    /\ state[leaderNode] = Leader
    /\ leaderNode # follower
    /\ term[leaderNode] >= term[follower]
    /\ state' = [state EXCEPT ![follower] = Follower]
    /\ leader' = [leader EXCEPT ![follower] = leaderNode]
    /\ IF term[leaderNode] > term[follower]
       THEN /\ term' = [term EXCEPT ![follower] = term[leaderNode]]
            /\ votedFor' = [votedFor EXCEPT ![follower] = None]
            /\ votes' = [votes EXCEPT ![follower] = {}]
       ELSE UNCHANGED <<term, votedFor, votes>>

StepDown(n) ==
    /\ state[n] = Leader
    /\ \E other \in Nodes:
        /\ other # n
        /\ term[other] > term[n]
    /\ state' = [state EXCEPT ![n] = Follower]
    /\ leader' = [leader EXCEPT ![n] = None]
    /\ UNCHANGED <<term, votedFor, votes>>

Next ==
    \/ \E n \in Nodes: Timeout(n)
    \/ \E c, v \in Nodes: RequestVote(c, v)
    \/ \E n \in Nodes: BecomeLeader(n)
    \/ \E l, f \in Nodes: Heartbeat(l, f)
    \/ \E n \in Nodes: StepDown(n)

TypeOK ==
    /\ state \in [Nodes -> {Follower, Candidate, Leader}]
    /\ term \in [Nodes -> 0..MaxTerm]
    /\ votedFor \in [Nodes -> Nodes \union {None}]
    /\ votes \in [Nodes -> SUBSET Nodes]
    /\ leader \in [Nodes -> Nodes \union {None}]

InvAtMostOneLeaderPerTerm ==
    \A n1 \in Nodes: \A n2 \in Nodes:
        (state[n1] = Leader /\ state[n2] = Leader /\ n1 # n2)
        => term[n1] # term[n2]

InvLeaderHasQuorum ==
    \A n \in Nodes:
        state[n] = Leader => HasQuorum(n)

InvCandidateVotedForSelf ==
    \A n \in Nodes:
        state[n] = Candidate => votedFor[n] = n

====
