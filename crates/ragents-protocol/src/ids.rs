use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

macro_rules! define_string_id {
    ($name:ident, $prefix:expr) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(raw: impl Into<String>) -> Self {
                Self(raw.into())
            }

            pub fn generate() -> Self {
                Self(format!("{}{}", $prefix, Uuid::now_v7()))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn is_safe_identifier(&self) -> bool {
                !self.0.trim().is_empty()
                    && !self.0.contains('/')
                    && !self.0.contains('\\')
                    && !self.0.contains("..")
                    && !self.0.contains('\0')
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_string())
            }
        }
    };
}

define_string_id!(NodeId, "node_");
define_string_id!(SessionId, "sess_");
define_string_id!(ActivationId, "act_");
define_string_id!(TurnId, "turn_");
define_string_id!(EventId, "evt_");
define_string_id!(PermissionRequestId, "permreq_");
define_string_id!(CommandId, "cmd_");
define_string_id!(ConfigRef, "cfg_sha256:");
define_string_id!(PermissionSnapshotRef, "permsnap_sha256:");

macro_rules! define_seq_newtype {
    ($name:ident) => {
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Default,
            Serialize,
            Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub u64);

        impl $name {
            pub const fn new(val: u64) -> Self {
                Self(val)
            }

            pub const fn value(self) -> u64 {
                self.0
            }

            pub const fn next(self) -> Self {
                Self(self.0.saturating_add(1))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self(value)
            }
        }
    };
}

define_seq_newtype!(ResourceVersion);
define_seq_newtype!(Generation);
define_seq_newtype!(LeaseEpoch);
define_seq_newtype!(BatchSeq);
define_seq_newtype!(LocalItemSeq);
define_seq_newtype!(EventSeq);
define_seq_newtype!(ContextPos);
