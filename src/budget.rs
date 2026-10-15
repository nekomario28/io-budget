//! The budget itself: how much time is left, and the rule that it never reads
//! as zero.

use crate::clock::{Clock, Nanos};
use std::time::Duration;

/// One absolute deadline, checked against an injected clock.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Budget {
    deadline: Nanos,
}

impl Budget {
    pub fn until(deadline: Nanos) -> Self {
        Self { deadline }
    }

    /// A budget of `duration` from `clock.now()`.
    pub fn for_duration(duration: Duration, clock: &impl Clock) -> Self {
        Self {
            deadline: clock.now() + duration,
        }
    }

    pub fn deadline(&self) -> Nanos {
        self.deadline
    }

    pub fn expired(&self, now: Nanos) -> bool {
        now >= self.deadline
    }

    /// Time left, never zero, or an expired-budget error.
    ///
    /// A zero timeout is not "do not wait" to every stack: some USB and socket
    /// implementations read it as "wait forever", which turns an expired budget
    /// into an unbounded one. One millisecond is the floor.
    pub fn remaining(&self, now: Nanos) -> std::io::Result<Duration> {
        if self.expired(now) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "deadline expired",
            ));
        }
        Ok(self
            .deadline
            .saturating_sub(now)
            .as_duration()
            .max(Duration::from_millis(1)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget() -> Budget {
        Budget::until(Nanos(1_000_000_000))
    }

    #[test]
    fn remaining_is_the_difference_and_never_zero() {
        let left = budget().remaining(Nanos(999_999_000)).unwrap();
        assert_eq!(
            left,
            Duration::from_micros(1_000).max(Duration::from_millis(1))
        );
        // The last microsecond still reads as one millisecond, never zero.
        assert_eq!(
            budget().remaining(Nanos(999_999_999)).unwrap(),
            Duration::from_millis(1)
        );
    }

    #[test]
    fn an_expired_budget_is_an_error_not_a_zero_timeout() {
        let error = budget().remaining(Nanos(1_000_000_000)).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        assert!(budget().remaining(Nanos(2_000_000_000)).is_err());
    }

    #[test]
    fn a_duration_budget_is_measured_from_the_injected_clock() {
        let clock = crate::clock::ManualClock::new(Nanos(500));
        let budget = Budget::for_duration(Duration::from_millis(10), &clock);
        assert_eq!(budget.deadline(), Nanos(500) + Duration::from_millis(10));
        clock.advance(Duration::from_millis(9));
        assert!(budget.remaining(clock.now()).is_ok());
        clock.advance(Duration::from_millis(2));
        assert!(budget.remaining(clock.now()).is_err());
    }
}
