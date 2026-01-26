---- MODULE RaftElection ----
EXTENDS Naturals, FiniteSets

CONSTANTS Nodes, MaxTerm

VARIABLES
    state,
    term,
    votedFor,
    votes,
    leader,
    network

vars == <<state, term, votedFor, votes, leader, network>>

Follower == "Follower"
Candidate == "Candidate"
Leader == "Leader"
None == "None"

RequestVoteReq == "RequestVoteReq"
RequestVoteResp == "RequestVoteResp"
AppendEntries == "AppendEntries"

TypeOK ==
    /\ state \in [Nodes -> {Follower, Candidate, Leader}]
    /\ term \in [Nodes -> 0..MaxTerm]
    /\ votedFor \in [Nodes -> Nodes \union {None}]
    /\ votes \in [Nodes -> SUBSET Nodes]
    /\ leader \in [Nodes -> Nodes \union {None}]
    /\ network \subseteq [type: {RequestVoteReq, RequestVoteResp, AppendEntries},
                           from: Nodes,
                           to: Nodes,
                           term: 0..MaxTerm,
                           granted: BOOLEAN]

QuorumSize == (Cardinality(Nodes) \div 2) + 1

HasQuorum(n) == Cardinality(votes[n]) >= QuorumSize

Init ==
    /\ state = [n \in Nodes |-> Follower]
    /\ term = [n \in Nodes |-> 0]
    /\ votedFor = [n \in Nodes |-> None]
    /\ votes = [n \in Nodes |-> {}]
    /\ leader = [n \in Nodes |-> None]
    /\ network = {}

BecomeCandidate(n) ==
    /\ state[n] \in {Follower, Candidate}
    /\ term[n] < MaxTerm
    /\ state' = [state EXCEPT ![n] = Candidate]
    /\ term' = [term EXCEPT ![n] = term[n] + 1]
    /\ votedFor' = [votedFor EXCEPT ![n] = n]
    /\ votes' = [votes EXCEPT ![n] = {n}]
    /\ leader' = [leader EXCEPT ![n] = None]
    /\ UNCHANGED network

SendRequestVote(n, peer) ==
    /\ state[n] = Candidate
    /\ peer \in Nodes
    /\ peer # n
    /\ network' = network \union {[type |-> RequestVoteReq,
                                    from |-> n,
                                    to |-> peer,
                                    term |-> term[n],
                                    granted |-> FALSE]}
    /\ UNCHANGED <<state, term, votedFor, votes, leader>>

HandleRequestVote(n) ==
    /\ \E msg \in network:
        /\ msg.type = RequestVoteReq
        /\ msg.to = n
        /\ LET senderTerm == msg.term
               sender == msg.from
               canGrant == /\ (senderTerm >= term[n])
                          /\ (votedFor[n] = None \/ votedFor[n] = sender)
           IN
           IF senderTerm > term[n]
           THEN
               /\ state' = [state EXCEPT ![n] = Follower]
               /\ term' = [term EXCEPT ![n] = senderTerm]
               /\ votedFor' = [votedFor EXCEPT ![n] = sender]
               /\ votes' = [votes EXCEPT ![n] = {}]
               /\ leader' = [leader EXCEPT ![n] = None]
               /\ network' = (network \ {msg}) \union
                             {[type |-> RequestVoteResp,
                               from |-> n,
                               to |-> sender,
                               term |-> senderTerm,
                               granted |-> TRUE]}
           ELSE IF canGrant
           THEN
               /\ votedFor' = [votedFor EXCEPT ![n] = sender]
               /\ network' = (network \ {msg}) \union
                             {[type |-> RequestVoteResp,
                               from |-> n,
                               to |-> sender,
                               term |-> term[n],
                               granted |-> TRUE]}
               /\ UNCHANGED <<state, term, votes, leader>>
           ELSE
               /\ network' = (network \ {msg}) \union
                             {[type |-> RequestVoteResp,
                               from |-> n,
                               to |-> sender,
                               term |-> term[n],
                               granted |-> FALSE]}
               /\ UNCHANGED <<state, term, votedFor, votes, leader>>

HandleRequestVoteResponse(n) ==
    /\ \E msg \in network:
        /\ msg.type = RequestVoteResp
        /\ msg.to = n
        /\ IF msg.term > term[n]
           THEN
               /\ state' = [state EXCEPT ![n] = Follower]
               /\ term' = [term EXCEPT ![n] = msg.term]
               /\ votedFor' = [votedFor EXCEPT ![n] = None]
               /\ votes' = [votes EXCEPT ![n] = {}]
               /\ leader' = [leader EXCEPT ![n] = None]
               /\ network' = network \ {msg}
           ELSE IF /\ state[n] = Candidate
                   /\ msg.term = term[n]
                   /\ msg.granted
           THEN
               /\ votes' = [votes EXCEPT ![n] = votes[n] \union {msg.from}]
               /\ network' = network \ {msg}
               /\ UNCHANGED <<state, term, votedFor, leader>>
           ELSE
               /\ network' = network \ {msg}
               /\ UNCHANGED <<state, term, votedFor, votes, leader>>

BecomeLeader(n) ==
    /\ state[n] = Candidate
    /\ HasQuorum(n)
    /\ state' = [state EXCEPT ![n] = Leader]
    /\ leader' = [leader EXCEPT ![n] = n]
    /\ UNCHANGED <<term, votedFor, votes, network>>

SendAppendEntries(n, peer) ==
    /\ state[n] = Leader
    /\ peer \in Nodes
    /\ peer # n
    /\ network' = network \union {[type |-> AppendEntries,
                                    from |-> n,
                                    to |-> peer,
                                    term |-> term[n],
                                    granted |-> FALSE]}
    /\ UNCHANGED <<state, term, votedFor, votes, leader>>

HandleAppendEntries(n) ==
    /\ \E msg \in network:
        /\ msg.type = AppendEntries
        /\ msg.to = n
        /\ LET senderTerm == msg.term
               sender == msg.from
           IN
           IF senderTerm < term[n]
           THEN
               /\ network' = network \ {msg}
               /\ UNCHANGED <<state, term, votedFor, votes, leader>>
           ELSE IF senderTerm > term[n] \/ state[n] # Follower
           THEN
               /\ state' = [state EXCEPT ![n] = Follower]
               /\ term' = [term EXCEPT ![n] = senderTerm]
               /\ votedFor' = [votedFor EXCEPT ![n] = None]
               /\ votes' = [votes EXCEPT ![n] = {}]
               /\ leader' = [leader EXCEPT ![n] = sender]
               /\ network' = network \ {msg}
           ELSE
               /\ leader' = [leader EXCEPT ![n] = sender]
               /\ network' = network \ {msg}
               /\ UNCHANGED <<state, term, votedFor, votes>>

DropMessage ==
    /\ network # {}
    /\ \E msg \in network:
        /\ network' = network \ {msg}
    /\ UNCHANGED <<state, term, votedFor, votes, leader>>

Next ==
    \/ \E n \in Nodes: BecomeCandidate(n)
    \/ \E n \in Nodes: \E peer \in Nodes: SendRequestVote(n, peer)
    \/ \E n \in Nodes: HandleRequestVote(n)
    \/ \E n \in Nodes: HandleRequestVoteResponse(n)
    \/ \E n \in Nodes: BecomeLeader(n)
    \/ \E n \in Nodes: \E peer \in Nodes: SendAppendEntries(n, peer)
    \/ \E n \in Nodes: HandleAppendEntries(n)
    \/ DropMessage

InvAtMostOneLeaderPerTerm ==
    \A n1 \in Nodes: \A n2 \in Nodes:
        (state[n1] = Leader /\ state[n2] = Leader /\ n1 # n2)
        => term[n1] # term[n2]

InvLeaderHasQuorum ==
    \A n \in Nodes:
        state[n] = Leader => HasQuorum(n)

InvVotedForConsistency ==
    \A n \in Nodes:
        votedFor[n] # None => term[n] > 0 \/ votedFor[n] = n

InvCandidateVotedForSelf ==
    \A n \in Nodes:
        state[n] = Candidate => votedFor[n] = n

InvLeaderKnowsSelf ==
    \A n \in Nodes:
        state[n] = Leader => leader[n] = n

InvTermMonotonic ==
    \A msg \in network:
        msg.term <= MaxTerm

====
