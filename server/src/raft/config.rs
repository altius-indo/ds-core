//! Raft timing and its validation (REQ-0018, design/raft-ranges.md §7).
//!
//! The election timeout must be at least 10× the measured heartbeat round trip (REQ-0018 AC2):
//! shorter timeouts turn ordinary network jitter into spurious elections.
//!
//! Measured kill-to-first-write is about twice `election_max`: followers first honour the dead
//! leader's lease, then elect. The drafted 3–6 s range gave p99 11.7 s (REQ-0018 AC1 allows
//! 10 s), so the defaults are 150 ms / 1.5–3 s, which measured p99 6.1 s over 20 trials
//! (`dscore-harness fault leader-kill`). They pass AC2 for round trips up to 150 ms.

// reqforge: implements REQ-0018

use std::fmt;
use std::time::Duration;

use openraft::Config;

/// REQ-0018 AC2.
pub const MIN_TIMEOUT_TO_RTT: u32 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
    pub heartbeat: Duration,
    /// Election timeouts are drawn uniformly from `[election_min, election_max)`.
    pub election_min: Duration,
    pub election_max: Duration,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            heartbeat: Duration::from_millis(150),
            election_min: Duration::from_millis(1500),
            election_max: Duration::from_millis(3000),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimingError {
    /// `election_min` is below 10× the measured heartbeat round trip.
    ElectionBelowRtt {
        election_min: Duration,
        rtt: Duration,
        required: Duration,
    },
    /// The heartbeat must fire several times per election timeout.
    HeartbeatTooSlow {
        heartbeat: Duration,
        election_min: Duration,
    },
    EmptyElectionRange,
}

impl fmt::Display for TimingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ElectionBelowRtt {
                election_min,
                rtt,
                required,
            } => write!(
                f,
                "election timeout {election_min:?} is below {MIN_TIMEOUT_TO_RTT}x the measured heartbeat round trip {rtt:?}; use at least {required:?}"
            ),
            Self::HeartbeatTooSlow {
                heartbeat,
                election_min,
            } => write!(
                f,
                "heartbeat interval {heartbeat:?} must be at most a third of the election timeout {election_min:?}"
            ),
            Self::EmptyElectionRange => {
                f.write_str("election_max must be greater than election_min")
            }
        }
    }
}

impl std::error::Error for TimingError {}

impl Timing {
    /// Validate against a measured heartbeat round trip (REQ-0018 AC2). Run at startup with the
    /// round trip measured to peers, and on every configuration change.
    pub fn validate(&self, measured_heartbeat_rtt: Duration) -> Result<(), TimingError> {
        if self.election_max <= self.election_min {
            return Err(TimingError::EmptyElectionRange);
        }
        if self.heartbeat * 3 > self.election_min {
            return Err(TimingError::HeartbeatTooSlow {
                heartbeat: self.heartbeat,
                election_min: self.election_min,
            });
        }
        let required = measured_heartbeat_rtt * MIN_TIMEOUT_TO_RTT;
        if self.election_min < required {
            return Err(TimingError::ElectionBelowRtt {
                election_min: self.election_min,
                rtt: measured_heartbeat_rtt,
                required,
            });
        }
        Ok(())
    }

    pub fn to_openraft(&self) -> Config {
        Config {
            heartbeat_interval: self.heartbeat.as_millis() as u64,
            election_timeout_min: self.election_min.as_millis() as u64,
            election_timeout_max: self.election_max.as_millis() as u64,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod election_timeout {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    // reqforge: verifies REQ-0018#AC2
    #[test]
    fn rejects_timeout_below_10x_heartbeat_rtt() {
        let t = Timing::default(); // 1500 ms minimum
        assert!(t.validate(ms(150)).is_ok(), "exactly 10x is allowed");
        assert_eq!(
            t.validate(ms(151)),
            Err(TimingError::ElectionBelowRtt {
                election_min: ms(1500),
                rtt: ms(151),
                required: ms(1510),
            })
        );
    }

    // reqforge: verifies REQ-0018#AC2
    #[test]
    fn accepts_defaults_on_a_normal_network() {
        assert!(Timing::default().validate(ms(2)).is_ok());
        assert!(Timing::default().validate(ms(100)).is_ok());
    }

    #[test]
    fn rejects_inconsistent_ranges() {
        let mut t = Timing::default();
        t.election_max = t.election_min;
        assert_eq!(t.validate(ms(1)), Err(TimingError::EmptyElectionRange));
        let t = Timing {
            heartbeat: ms(800),
            ..Timing::default()
        };
        assert!(matches!(
            t.validate(ms(1)),
            Err(TimingError::HeartbeatTooSlow { .. })
        ));
    }

    #[test]
    fn default_timing_maps_to_openraft() {
        let c = Timing::default().to_openraft();
        assert_eq!(
            (
                c.heartbeat_interval,
                c.election_timeout_min,
                c.election_timeout_max
            ),
            (150, 1500, 3000)
        );
        assert!(c.validate().is_ok());
    }
}
