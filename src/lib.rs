//! One wall-clock budget for socket I/O, enforced *below* buffering layers.
//!
//! # The invariant this crate exists for
//!
//! A deadline is only a control if it is checked **before** an I/O operation.
//! Checking it *after* a successful read or write is worse than not checking at
//! all: the bytes the kernel already delivered are discarded, and beneath a
//! buffering layer the stream desynchronizes.
//!
//! Observed in production code: a TLS client checked its deadline after every
//! underlying read and write, so a read that completed as the deadline expired
//! had its bytes dropped. rustls keeps no copy. The record stream shifted and a
//! later record failed to decrypt — reported as
//! `cannot decrypt peer's message`, which names neither the timeout nor the
//! ordering. It failed roughly one run in five, so single green runs looked
//! like fixes.
//!
//! [`DeadlineStream`] encodes the correct order in code, and its regression
//! test reproduces the failure **deterministically** by advancing an injected
//! clock during the read. A post-operation check reintroduced into
//! [`DeadlineStream`] fails that test.
//!
//! # What else is here
//!
//! - [`Budget`]: remaining time on a deadline, never zero, because some USB and
//!   socket stacks read a zero timeout as "wait forever".
//! - [`frame::FrameReader`]: a fixed-size header plus a counted payload, where a
//!   poll timeout retains partial progress and one frame has one lifetime, so a
//!   trickled frame cannot renew its own budget.
//!
//! # What is deliberately not here
//!
//! No protocol. No pairing handshake, no header layout, no frame types. Those
//! belong to the caller; this crate owns the timing and partial-progress rules
//! that are easy to get wrong and invisible when wrong.
//!
//! The budget is a synchronous, threaded-code tool. Async runtimes solve this
//! with timeout combinators around a buffered stream; this crate is for code
//! that does not want a runtime.

mod budget;
mod clock;
mod deadline;
mod frame;

pub use budget::Budget;
pub use clock::{Clock, ManualClock, Nanos, SystemClock};
pub use deadline::{DeadlineStream, Timeoutable};
pub use frame::{Framed, FrameReader};
