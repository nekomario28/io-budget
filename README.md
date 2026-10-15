# io-budget

One wall-clock budget for socket I/O, enforced **below** buffering layers, plus
partial-frame retention that never loses a frame boundary.

## The bug this exists for

A deadline is only a control if it is checked **before** an I/O operation.

Checking it *after* a successful read or write is worse than not checking at
all. The bytes the kernel already delivered are discarded, and beneath a
buffering layer the stream desynchronizes:

```rust
// WRONG — and it looks more careful than the correct version.
let count = stream.read(bytes)?;
self.budget.remaining(clock.now())?;   // discards `count` bytes if this fails
Ok(count)
```

Under rustls that surfaces as `cannot decrypt peer's message` on a *later*
record. It names neither the timeout nor the ordering, and it fails
intermittently — in the project this was extracted from, roughly one run in
five, so single green runs kept looking like fixes.

```rust
// RIGHT
let left = self.budget.remaining(self.clock.now())?;
stream.set_read_timeout(Some(left))?;
stream.read(bytes)                      // returned untouched
```

`DeadlineStream` puts the correct order in code, and its regression test
reproduces the wrong one **deterministically**: the mock stream advances an
injected clock while the read is in flight, so the deadline expires exactly as
the bytes arrive.

## Contents

| Item | What it guarantees |
| --- | --- |
| [`Budget`](src/budget.rs) | Remaining time on a deadline, never zero — some USB and socket stacks read a zero timeout as "wait forever" |
| [`DeadlineStream`](src/deadline.rs) | Every read and write draws from one budget, installed on the underlying socket before the operation |
| [`FrameReader`](src/frame.rs) | A poll timeout is not a frame boundary: partial header/payload progress is retained, and one frame has one lifetime so a trickled frame cannot renew its own budget |
| [`Clock`](src/clock.rs) / `ManualClock` | Injectable monotonic time, because `Instant` cannot be constructed and the interesting case is untestable without it |

## Verified

```
cargo test   →  16 passed, 0 failed
```

The guard is proven able to fail. Reintroducing the post-operation check into
`DeadlineStream::read` — one line, the exact production shape — fails:

```
---- deadline::tests::delivered_bytes_survive_a_deadline_expiring_mid_read ----
delivered bytes were discarded: Custom { kind: TimedOut, error: "deadline expired" }
```

Restoring the correct order passes again. The other tests pin the rest of the
contract: the budget reaches the socket on every operation and shrinks as it is
spent; an expired budget touches the stream not at all; a frame split across
polls keeps its boundary for every chunk size; consecutive frames do not bleed;
an over-ceiling payload count is rejected before anything is allocated; a closed
peer is an error, not a short frame; an interrupted read retries without losing
progress.

## Why a crate, and why sync

The need is measurable. On GitHub, 88 issue titles combine `set_read_timeout`
with rustls and 909 combine rustls with `WouldBlock` — people reaching for the
socket-level timeout and meeting rustls' internal buffering. The ecosystem's
answer is async: `tokio-io-timeout` has 133 million downloads. The one prior
sync attempt, `timeout_io`, is at version 0.0.0.

So this is not "timeouts for io" — that name lost. It is the **ordering
invariant, made testable**, for threaded code that does not want a runtime. The
injectable clock is the differentiator: without it, the regression is a flake,
and a flake is not a proof.

## Deliberately not included

No protocol. No header layout, no frame types, no pairing handshake. Those
belong to the caller. This crate owns the timing and partial-progress rules,
which are easy to get wrong and invisible when wrong.

## Status

Extracted from [`mobile-webcam-bridge`](https://github.com/nekomario28/mobile-webcam-bridge),
which consumes it by path. Not published to crates.io; that is a separate
release decision.

MSRV 1.75. No dependencies. Apache-2.0.
