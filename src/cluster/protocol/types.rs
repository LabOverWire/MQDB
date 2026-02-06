// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    Heartbeat = 0,
    DetailedHeartbeat = 1,
    DeathNotice = 2,
    ReplicationWrite = 10,
    ReplicationAck = 11,
    CatchupRequest = 12,
    CatchupResponse = 13,
    WriteRequest = 20,
    ForwardedPublish = 30,
    SnapshotRequest = 40,
    SnapshotChunk = 41,
    SnapshotComplete = 42,
    QueryRequest = 50,
    QueryResponse = 51,
    BatchReadRequest = 52,
    BatchReadResponse = 53,
    JsonDbRequest = 54,
    JsonDbResponse = 55,
    UniqueReserveRequest = 80,
    UniqueReserveResponse = 81,
    UniqueCommitRequest = 82,
    UniqueCommitResponse = 83,
    UniqueReleaseRequest = 84,
    UniqueReleaseResponse = 85,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Operation {
    Insert = 0,
    Update = 1,
    Delete = 2,
}

impl Operation {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Insert),
            1 => Some(Self::Update),
            2 => Some(Self::Delete),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AckStatus {
    Ok = 0,
    StaleEpoch = 1,
    NotReplica = 2,
    SequenceGap = 3,
}

impl AckStatus {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Ok),
            1 => Some(Self::StaleEpoch),
            2 => Some(Self::NotReplica),
            3 => Some(Self::SequenceGap),
            _ => None,
        }
    }

    #[must_use]
    pub fn is_ok(self) -> bool {
        self == Self::Ok
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum QueryStatus {
    Ok = 0,
    Timeout = 1,
    Error = 2,
    NotPrimary = 3,
}

impl QueryStatus {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Ok),
            1 => Some(Self::Timeout),
            2 => Some(Self::Error),
            3 => Some(Self::NotPrimary),
            _ => None,
        }
    }

    #[must_use]
    pub fn is_ok(self) -> bool {
        self == Self::Ok
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum JsonDbOp {
    Read = 0,
    Update = 1,
    Delete = 2,
    List = 3,
    Create = 4,
}

impl JsonDbOp {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Read),
            1 => Some(Self::Update),
            2 => Some(Self::Delete),
            3 => Some(Self::List),
            4 => Some(Self::Create),
            _ => None,
        }
    }
}
