//! Monotonic time that a test can place anywhere.
//!
//! `std::time::Instant` cannot be constructed from a value, so a deadline built
//! on it cannot be tested for the case that matters: an operation that succeeds
//! *as* the budget expires. Nanoseconds since an arbitrary origin can.

/// Monotonic nanoseconds. Not [`std::time::Instant`], so a test can set it.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct Nanos(pub u128);

impl Nanos {
    pub const ZERO: Self = Self(0);

    pub fn from_duration(duration: std::time::Duration) -> Self {
        Self(duration.as_nanos())
    }

    pub fn as_duration(self) -> std::time::Duration {
        std::time::Duration::from_nanos(self.0.min(u64::MAX as u128) as u64)
    }

    pub fn saturating_sub(self, earlier: Self) -> Self {
        Self(self.0.saturating_sub(earlier.0))
    }
}

impl std::ops::Add<std::time::Duration> for Nanos {
    type Output = Self;
    fn add(self, duration: std::time::Duration) -> Self {
        Self(self.0.saturating_add(duration.as_nanos()))
    }
}

impl std::ops::Sub for Nanos {
    type Output = Self;
    fn sub(self, earlier: Self) -> Self {
        Self(self.0.saturating_sub(earlier.0))
    }
}

/// A source of monotonic time, injectable so timing is testable.
pub trait Clock {
    fn now(&self) -> Nanos;
}

/// The monotonic clock.
///
/// Anchored to the first reading rather than to the wall clock: a deadline
/// control must not be defeated by an NTP step or a user changing the system
/// time, and `SystemTime` can move backwards.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

fn anchor() -> std::time::Instant {
    static ANCHOR: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    *ANCHOR.get_or_init(std::time::Instant::now)
}

impl Clock for SystemClock {
    fn now(&self) -> Nanos {
        Nanos(anchor().elapsed().as_nanos())
    }
}

/// A clock a test moves by hand. The whole point of the crate's testability.
#[derive(Clone, Debug, Default)]
pub struct ManualClock(std::sync::Arc<std::sync::Mutex<u128>>);

impl ManualClock {
    pub fn new(at: Nanos) -> Self {
        Self(std::sync::Arc::new(std::sync::Mutex::new(at.0)))
    }

    pub fn set(&self, at: Nanos) {
        *self.0.lock().unwrap() = at.0;
    }

    pub fn advance(&self, by: std::time::Duration) {
        let mut now = self.0.lock().unwrap();
        *now = now.saturating_add(by.as_nanos());
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Nanos {
        Nanos(*self.0.lock().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_manual_clock_only_moves_when_told() {
        let clock = ManualClock::new(Nanos(1_000));
        assert_eq!(clock.now(), Nanos(1_000));
        clock.advance(std::time::Duration::from_millis(250));
        assert_eq!(clock.now(), Nanos(250_001_000));
        clock.set(Nanos(7));
        assert_eq!(clock.now(), Nanos(7));
    }

    #[test]
    fn the_system_clock_only_moves_forward() {
        let first = SystemClock.now();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let second = SystemClock.now();
        assert!(second > first, "{first:?} then {second:?}");
        assert!(second - first >= Nanos(5_000_000));
    }

    #[test]
    fn a_manual_clock_is_shared_through_a_clone() {
        let clock = ManualClock::default();
        let alias = clock.clone();
        clock.advance(std::time::Duration::from_secs(1));
        assert_eq!(alias.now(), clock.now());
    }
}
