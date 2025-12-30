use super::PartitionId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbTopicOperation {
    Create { entity: String },
    Read { entity: String, id: String },
    Update { entity: String, id: String },
    Delete { entity: String, id: String },
    IndexUpdate,
    UniqueReserve,
    UniqueCommit,
    UniqueRelease,
    FkValidate,
    QueryRequest { query_id: String },
    QueryResponse { query_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedDbTopic {
    pub partition: Option<PartitionId>,
    pub operation: DbTopicOperation,
}

impl ParsedDbTopic {
    #[must_use]
    pub fn parse(topic: &str) -> Option<Self> {
        if !topic.starts_with("$DB/") {
            return None;
        }

        let rest = &topic[4..];
        let parts: Vec<&str> = rest.split('/').collect();

        if parts.is_empty() {
            return None;
        }

        match parts[0] {
            s if s.starts_with('p') => Self::parse_partition_topic(&parts),
            "_idx" => Self::parse_index_topic(&parts),
            "_unique" => Self::parse_unique_topic(&parts),
            "_fk" => Self::parse_fk_topic(&parts),
            "_query" => Self::parse_query_topic(&parts),
            _ => None,
        }
    }

    fn parse_partition_topic(parts: &[&str]) -> Option<Self> {
        if parts.len() < 2 {
            return None;
        }

        let partition = Self::parse_partition_id(parts[0])?;
        let entity = parts[1].to_string();

        match parts.len() {
            3 => {
                let op_or_id = parts[2];
                if op_or_id == "create" {
                    Some(Self {
                        partition: Some(partition),
                        operation: DbTopicOperation::Create { entity },
                    })
                } else {
                    Some(Self {
                        partition: Some(partition),
                        operation: DbTopicOperation::Read {
                            entity,
                            id: op_or_id.to_string(),
                        },
                    })
                }
            }
            4 => {
                let id = parts[2].to_string();
                match parts[3] {
                    "update" => Some(Self {
                        partition: Some(partition),
                        operation: DbTopicOperation::Update { entity, id },
                    }),
                    "delete" => Some(Self {
                        partition: Some(partition),
                        operation: DbTopicOperation::Delete { entity, id },
                    }),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn parse_index_topic(parts: &[&str]) -> Option<Self> {
        if parts.len() != 3 || parts[2] != "update" {
            return None;
        }

        let partition = Self::parse_partition_id(parts[1])?;
        Some(Self {
            partition: Some(partition),
            operation: DbTopicOperation::IndexUpdate,
        })
    }

    fn parse_unique_topic(parts: &[&str]) -> Option<Self> {
        if parts.len() != 3 {
            return None;
        }

        let partition = Self::parse_partition_id(parts[1])?;
        let operation = match parts[2] {
            "reserve" => DbTopicOperation::UniqueReserve,
            "commit" => DbTopicOperation::UniqueCommit,
            "release" => DbTopicOperation::UniqueRelease,
            _ => return None,
        };

        Some(Self {
            partition: Some(partition),
            operation,
        })
    }

    fn parse_fk_topic(parts: &[&str]) -> Option<Self> {
        if parts.len() != 3 || parts[2] != "validate" {
            return None;
        }

        let partition = Self::parse_partition_id(parts[1])?;
        Some(Self {
            partition: Some(partition),
            operation: DbTopicOperation::FkValidate,
        })
    }

    fn parse_query_topic(parts: &[&str]) -> Option<Self> {
        if parts.len() != 3 {
            return None;
        }

        let query_id = parts[1].to_string();
        let operation = match parts[2] {
            "request" => DbTopicOperation::QueryRequest { query_id },
            "response" => DbTopicOperation::QueryResponse { query_id },
            _ => return None,
        };

        Some(Self {
            partition: None,
            operation,
        })
    }

    fn parse_partition_id(s: &str) -> Option<PartitionId> {
        if !s.starts_with('p') {
            return None;
        }
        let num: u16 = s[1..].parse().ok()?;
        PartitionId::new(num)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_topic() {
        let parsed = ParsedDbTopic::parse("$DB/p17/users/create").unwrap();
        assert_eq!(parsed.partition, PartitionId::new(17));
        assert_eq!(
            parsed.operation,
            DbTopicOperation::Create {
                entity: "users".to_string()
            }
        );
    }

    #[test]
    fn parse_read_topic() {
        let parsed = ParsedDbTopic::parse("$DB/p5/orders/ord-123").unwrap();
        assert_eq!(parsed.partition, PartitionId::new(5));
        assert_eq!(
            parsed.operation,
            DbTopicOperation::Read {
                entity: "orders".to_string(),
                id: "ord-123".to_string()
            }
        );
    }

    #[test]
    fn parse_update_topic() {
        let parsed = ParsedDbTopic::parse("$DB/p42/users/user-456/update").unwrap();
        assert_eq!(parsed.partition, PartitionId::new(42));
        assert_eq!(
            parsed.operation,
            DbTopicOperation::Update {
                entity: "users".to_string(),
                id: "user-456".to_string()
            }
        );
    }

    #[test]
    fn parse_delete_topic() {
        let parsed = ParsedDbTopic::parse("$DB/p0/sessions/sess-789/delete").unwrap();
        assert_eq!(parsed.partition, PartitionId::new(0));
        assert_eq!(
            parsed.operation,
            DbTopicOperation::Delete {
                entity: "sessions".to_string(),
                id: "sess-789".to_string()
            }
        );
    }

    #[test]
    fn parse_index_update_topic() {
        let parsed = ParsedDbTopic::parse("$DB/_idx/p33/update").unwrap();
        assert_eq!(parsed.partition, PartitionId::new(33));
        assert_eq!(parsed.operation, DbTopicOperation::IndexUpdate);
    }

    #[test]
    fn parse_unique_reserve_topic() {
        let parsed = ParsedDbTopic::parse("$DB/_unique/p10/reserve").unwrap();
        assert_eq!(parsed.partition, PartitionId::new(10));
        assert_eq!(parsed.operation, DbTopicOperation::UniqueReserve);
    }

    #[test]
    fn parse_unique_commit_topic() {
        let parsed = ParsedDbTopic::parse("$DB/_unique/p10/commit").unwrap();
        assert_eq!(parsed.partition, PartitionId::new(10));
        assert_eq!(parsed.operation, DbTopicOperation::UniqueCommit);
    }

    #[test]
    fn parse_unique_release_topic() {
        let parsed = ParsedDbTopic::parse("$DB/_unique/p10/release").unwrap();
        assert_eq!(parsed.partition, PartitionId::new(10));
        assert_eq!(parsed.operation, DbTopicOperation::UniqueRelease);
    }

    #[test]
    fn parse_fk_validate_topic() {
        let parsed = ParsedDbTopic::parse("$DB/_fk/p25/validate").unwrap();
        assert_eq!(parsed.partition, PartitionId::new(25));
        assert_eq!(parsed.operation, DbTopicOperation::FkValidate);
    }

    #[test]
    fn parse_query_request_topic() {
        let parsed = ParsedDbTopic::parse("$DB/_query/q-abc-123/request").unwrap();
        assert_eq!(parsed.partition, None);
        assert_eq!(
            parsed.operation,
            DbTopicOperation::QueryRequest {
                query_id: "q-abc-123".to_string()
            }
        );
    }

    #[test]
    fn parse_query_response_topic() {
        let parsed = ParsedDbTopic::parse("$DB/_query/q-abc-123/response").unwrap();
        assert_eq!(parsed.partition, None);
        assert_eq!(
            parsed.operation,
            DbTopicOperation::QueryResponse {
                query_id: "q-abc-123".to_string()
            }
        );
    }

    #[test]
    fn parse_invalid_topics() {
        assert!(ParsedDbTopic::parse("not/a/db/topic").is_none());
        assert!(ParsedDbTopic::parse("$DB/").is_none());
        assert!(ParsedDbTopic::parse("$DB/p99/users").is_none());
        assert!(ParsedDbTopic::parse("$DB/p64/users/create").is_none());
        assert!(ParsedDbTopic::parse("$DB/_unknown/p5/op").is_none());
    }

    #[test]
    fn partition_63_is_valid() {
        let parsed = ParsedDbTopic::parse("$DB/p63/test/create").unwrap();
        assert_eq!(parsed.partition, PartitionId::new(63));
    }

    #[test]
    fn partition_64_is_invalid() {
        assert!(ParsedDbTopic::parse("$DB/p64/test/create").is_none());
    }
}
