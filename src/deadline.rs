//! Enforcing one budget at the bottom of a buffering layer.
//!
//! The ordering rule lives here, in [`DeadlineStream::read`] and
//! [`DeadlineStream::write`]: check the budget, install it on the underlying
//! socket, perform the operation, return the result untouched.
//!
//! The order is the whole point. A check placed after the operation turns a
//! successful read into a discarded one, and under a buffering layer that
//! desynchronizes the stream. The regression test in this module reproduces
//! that failure deterministically.

use crate::budget::Budget;
use crate::clock::{Clock, SystemClock};
use std::io::{Read, Write};
use std::time::Duration;

/// A stream whose per-operation timeout can be installed.
///
/// Implemented for [`std::net::TcpStream`]; a buffering layer above this
/// (rustls, an HTTP/2 session, a frame reader) cannot see the budget, so it has
/// to be pushed down to here before every operation.
pub trait Timeoutable {
    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> std::io::Result<()>;
    fn set_write_timeout(&mut self, timeout: Option<Duration>) -> std::io::Result<()>;
}

impl Timeoutable for std::net::TcpStream {
    // Fully qualified: the inherent methods share this name, and an unqualified
    // call inside the trait implementation resolves back to the trait.
    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> std::io::Result<()> {
        std::net::TcpStream::set_read_timeout(self, timeout)
    }
    fn set_write_timeout(&mut self, timeout: Option<Duration>) -> std::io::Result<()> {
        std::net::TcpStream::set_write_timeout(self, timeout)
    }
}

/// A stream that draws every operation from one budget.
pub struct DeadlineStream<S, C = SystemClock> {
    stream: S,
    budget: Budget,
    clock: C,
}

impl<S, C: Clock> DeadlineStream<S, C> {
    pub fn new(stream: S, budget: Budget, clock: C) -> Self {
        Self {
            stream,
            budget,
            clock,
        }
    }

    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    pub fn get_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    pub fn into_inner(self) -> S {
        self.stream
    }

    /// Replace the budget without disturbing the stream. Useful when several
    /// operations belong to one larger transaction.
    pub fn set_budget(&mut self, budget: Budget) {
        self.budget = budget;
    }

    pub fn budget(&self) -> Budget {
        self.budget
    }

    pub fn remaining(&self) -> std::io::Result<Duration> {
        self.budget.remaining(self.clock.now())
    }
}

impl<S: Timeoutable + Read, C: Clock> Read for DeadlineStream<S, C> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        // Before, never after. See the module docs and `delivered_bytes_survive_a_deadline_expiring_mid_read`.
        let left = self.budget.remaining(self.clock.now())?;
        self.stream.set_read_timeout(Some(left))?;
        self.stream.read(bytes)
    }
}

impl<S: Timeoutable + Write, C: Clock> Write for DeadlineStream<S, C> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let left = self.budget.remaining(self.clock.now())?;
        self.stream.set_write_timeout(Some(left))?;
        self.stream.write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.budget.remaining(self.clock.now())?;
        self.stream.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{ManualClock, Nanos};
    use std::cell::RefCell;

    /// A stream that records the timeouts installed on it and can run a side
    /// effect as part of a read, which is how the ordering bug is reproduced.
    #[derive(Default)]
    struct Probe {
        read_timeouts: RefCell<Vec<Duration>>,
        write_timeouts: RefCell<Vec<Duration>>,
        reads: RefCell<Vec<usize>>,
        writes: RefCell<Vec<usize>>,
        on_read: RefCell<Option<Box<dyn FnMut()>>>,
        payload: Vec<u8>,
        fail_reads_with: RefCell<Option<std::io::ErrorKind>>,
    }

    impl Probe {
        fn with_payload(payload: &[u8]) -> Self {
            Self {
                payload: payload.to_vec(),
                ..Self::default()
            }
        }
    }

    impl Timeoutable for Probe {
        fn set_read_timeout(&mut self, timeout: Option<Duration>) -> std::io::Result<()> {
            self.read_timeouts
                .borrow_mut()
                .push(timeout.unwrap_or(Duration::MAX));
            Ok(())
        }
        fn set_write_timeout(&mut self, timeout: Option<Duration>) -> std::io::Result<()> {
            self.write_timeouts
                .borrow_mut()
                .push(timeout.unwrap_or(Duration::MAX));
            Ok(())
        }
    }

    impl Read for Probe {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            if let Some(effect) = self.on_read.borrow_mut().as_mut() {
                effect();
            }
            if let Some(kind) = *self.fail_reads_with.borrow() {
                return Err(std::io::Error::from(kind));
            }
            let count = bytes.len().min(self.payload.len());
            bytes[..count].copy_from_slice(&self.payload[..count]);
            self.payload.drain(..count);
            self.reads.borrow_mut().push(count);
            Ok(count)
        }
    }

    impl Write for Probe {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.writes.borrow_mut().push(bytes.len());
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn stream(
        probe: Probe,
        millis: u64,
        clock: &ManualClock,
    ) -> DeadlineStream<Probe, ManualClock> {
        let budget = Budget::until(clock.now() + std::time::Duration::from_millis(millis));
        DeadlineStream::new(probe, budget, clock.clone())
    }

    /// The regression this crate exists for.
    ///
    /// The read succeeds, and the clock passes the deadline while it is in
    /// flight — the exact shape of the production failure. The bytes must be
    /// delivered. A deadline check placed after the operation turns this into a
    /// lost read and fails the test.
    #[test]
    fn delivered_bytes_survive_a_deadline_expiring_mid_read() {
        let clock = ManualClock::new(Nanos(0));
        let probe = Probe::with_payload(b"AMB1");
        probe.on_read.borrow_mut().replace(Box::new({
            let clock = clock.clone();
            move || clock.advance(std::time::Duration::from_secs(10))
        }));
        let mut stream = stream(probe, 5, &clock);

        let mut bytes = [0u8; 4];
        let count = stream
            .read(&mut bytes)
            .expect("delivered bytes were discarded");
        assert_eq!(count, 4);
        assert_eq!(&bytes, b"AMB1");
        // The budget is spent: the next operation must fail.
        assert_eq!(
            stream.read(&mut bytes).unwrap_err().kind(),
            std::io::ErrorKind::TimedOut
        );
    }

    /// The budget really reaches the socket, which is the only way a buffering
    /// layer above cannot outlast it.
    #[test]
    fn the_remaining_budget_is_installed_on_every_operation() {
        let clock = ManualClock::new(Nanos(0));
        let probe = Probe::with_payload(b"12345678");
        let mut stream = stream(probe, 100, &clock);
        let mut bytes = [0u8; 4];

        stream.read_exact(&mut bytes).unwrap();
        clock.advance(std::time::Duration::from_millis(40));
        stream.read_exact(&mut bytes).unwrap();
        stream.write_all(b"ping").unwrap();

        let installed = stream.get_ref().read_timeouts.borrow().clone();
        assert_eq!(installed.len(), 2, "one install per read");
        assert_eq!(installed[0], Duration::from_millis(100));
        // The second read sees a budget already reduced by the first read's cost.
        assert!(installed[1] <= Duration::from_millis(60), "{installed:?}");
        assert_eq!(
            stream.get_ref().write_timeouts.borrow()[0],
            Duration::from_millis(60)
        );
    }

    /// An expired budget must not touch the stream at all.
    #[test]
    fn an_expired_budget_fails_before_any_operation() {
        let clock = ManualClock::new(Nanos(0));
        let probe = Probe::with_payload(b"data");
        let mut stream = stream(probe, 10, &clock);
        clock.advance(std::time::Duration::from_secs(1));

        let mut bytes = [0u8; 4];
        assert!(stream.read(&mut bytes).is_err());
        assert!(stream.write(b"data").is_err());
        assert!(stream.flush().is_err());
        assert!(stream.get_ref().reads.borrow().is_empty());
        assert!(stream.get_ref().writes.borrow().is_empty());
        assert!(stream.get_ref().read_timeouts.borrow().is_empty());
    }

    /// A partial read is not a failure and not a reason to renew anything.
    #[test]
    fn a_short_read_is_reported_as_a_short_read() {
        let clock = ManualClock::new(Nanos(0));
        let probe = Probe::with_payload(b"ab");
        let mut stream = stream(probe, 100, &clock);
        let mut bytes = [0u8; 8];
        assert_eq!(stream.read(&mut bytes).unwrap(), 2);
        assert_eq!(stream.read(&mut bytes).unwrap(), 0);
    }

    /// The budget can be swapped mid-transaction without disturbing the stream.
    #[test]
    fn a_new_budget_takes_effect_immediately() {
        let clock = ManualClock::new(Nanos(0));
        let probe = Probe::with_payload(b"xx");
        let mut stream = stream(probe, 1, &clock);
        clock.advance(std::time::Duration::from_millis(5));
        assert!(stream.read(&mut [0u8; 1]).is_err());

        stream.set_budget(Budget::until(clock.now() + Duration::from_secs(5)));
        assert_eq!(stream.read(&mut [0u8; 1]).unwrap(), 1);
    }
}
