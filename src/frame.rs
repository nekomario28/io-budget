//! Partial progress that survives a poll, with one lifetime per frame.
//!
//! A poll timeout is not a frame boundary. Retrying a read after a partial
//! header or payload has been consumed loses the frame: the next attempt reads
//! the remainder of the old frame as if it were a new header, and the stream is
//! garbage from there on. [`FrameReader`] owns the offsets instead.
//!
//! It also gives a frame one lifetime measured from its first byte, so a peer
//! that trickles a frame cannot extend its own deadline by keeping the
//! connection alive.

use std::io;
use std::time::{Duration, Instant};

/// The shape of a frame: a fixed-size header followed by a counted payload.
pub trait Framed {
    /// The decoded header, before its payload is read.
    type Header;
    /// The complete frame: header plus payload.
    type Frame;

    fn header_len() -> usize;
    fn decode_header(bytes: &[u8]) -> Result<Self::Header, io::Error>;

    /// Payload length, validated *before* anything is allocated or read.
    ///
    /// Returning an error here is the difference between rejecting an
    /// oversized count and allocating for it.
    fn payload_len(header: &Self::Header) -> Result<usize, io::Error>;

    /// Combine a decoded header with the payload bytes read for it.
    fn assemble(header: Self::Header, payload: Vec<u8>) -> Self::Frame;
}

/// One frame's worth of partial progress.
pub struct FrameReader<F: Framed> {
    header_bytes: Vec<u8>,
    header_offset: usize,
    header: Option<F::Header>,
    payload: Vec<u8>,
    payload_offset: usize,
    frame_started: Option<Instant>,
    max_frame_age: Duration,
}

impl<F: Framed> FrameReader<F> {
    /// `max_frame_age` is the lifetime of one partially received frame. A frame
    /// that cannot complete inside it terminates the connection rather than
    /// trickling indefinitely.
    pub fn new(max_frame_age: Duration) -> Self {
        Self {
            header_bytes: vec![0; F::header_len()],
            header_offset: 0,
            header: None,
            payload: Vec::new(),
            payload_offset: 0,
            frame_started: None,
            max_frame_age,
        }
    }

    pub fn max_frame_age(&self) -> Duration {
        self.max_frame_age
    }

    /// Whether a frame is partially received right now.
    pub fn partial(&self) -> bool {
        self.frame_started.is_some()
    }

    /// Read toward one complete frame.
    ///
    /// `read` receives the slice to fill and the time it may spend; it returns
    /// the byte count, or a timeout/`WouldBlock` error meaning "nothing more
    /// arrived". `Ok(None)` means the poll elapsed with the frame still
    /// incomplete, and the partial bytes are retained.
    pub fn poll(
        &mut self,
        timeout: Duration,
        mut read: impl FnMut(
            &mut [u8],
            Duration,
        ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>>,
    ) -> Result<Option<F::Frame>, Box<dyn std::error::Error + Send + Sync>> {
        let poll_deadline = Instant::now() + timeout;
        loop {
            let now = Instant::now();
            let frame_deadline = self.frame_started.map(|start| start + self.max_frame_age);
            if frame_deadline.is_some_and(|deadline| now >= deadline) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "incomplete frame deadline expired",
                )
                .into());
            }
            if self.header_offset == F::header_len() && self.header.is_none() {
                let header = F::decode_header(&self.header_bytes)?;
                self.payload = vec![0; F::payload_len(&header)?];
                self.header = Some(header);
            }
            if self.header.is_some() && self.payload_offset == self.payload.len() {
                let header = self.header.take().expect("header was just checked");
                let payload = std::mem::take(&mut self.payload);
                self.header_offset = 0;
                self.payload_offset = 0;
                self.frame_started = None;
                return Ok(Some(F::assemble(header, payload)));
            }
            let deadline = frame_deadline.map_or(poll_deadline, |frame| frame.min(poll_deadline));
            let Some(left) = deadline
                .checked_duration_since(now)
                .filter(|left| !left.is_zero())
            else {
                return Ok(None);
            };
            let target = if self.header.is_none() {
                &mut self.header_bytes[self.header_offset..]
            } else {
                &mut self.payload[self.payload_offset..]
            };
            let capacity = target.len();
            let count = match read(target, left.max(Duration::from_millis(1))) {
                Ok(0) => {
                    return Err(
                        io::Error::new(io::ErrorKind::UnexpectedEof, "peer disconnected").into(),
                    );
                }
                Ok(count) => count,
                Err(error)
                    if error.downcast_ref::<io::Error>().is_some_and(|error| {
                        matches!(
                            error.kind(),
                            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                        )
                    }) =>
                {
                    return Ok(None);
                }
                Err(error)
                    if error
                        .downcast_ref::<io::Error>()
                        .is_some_and(|error| error.kind() == io::ErrorKind::Interrupted) =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            };
            if count > capacity {
                return Err("read returned more bytes than its buffer".into());
            }
            self.frame_started.get_or_insert(now);
            if self.header.is_none() {
                self.header_offset += count;
            } else {
                self.payload_offset += count;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A four-byte magic header plus a one-byte counted payload: enough shape
    /// to exercise every branch without being a protocol.
    struct Shape;

    #[derive(Debug, PartialEq, Eq)]
    struct Header {
        magic: [u8; 4],
        len: u8,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Frame {
        header: Header,
        payload: Vec<u8>,
    }

    impl Frame {
        fn len(&self) -> u8 {
            self.header.len
        }
    }

    impl Framed for Shape {
        type Header = Header;
        type Frame = Frame;
        fn header_len() -> usize {
            5
        }
        fn decode_header(bytes: &[u8]) -> Result<Header, io::Error> {
            let magic: [u8; 4] = bytes[..4].try_into().unwrap();
            if &magic != b"TST1" {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
            }
            Ok(Header {
                magic,
                len: bytes[4],
            })
        }
        fn payload_len(header: &Header) -> Result<usize, io::Error> {
            if header.len > 16 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "count exceeds the payload ceiling",
                ));
            }
            Ok(header.len as usize)
        }
        fn assemble(header: Header, payload: Vec<u8>) -> Frame {
            Frame { header, payload }
        }
    }

    fn frame(len: u8) -> Vec<u8> {
        let mut bytes = b"TST1".to_vec();
        bytes.push(len);
        bytes.extend(std::iter::repeat(7u8).take(len as usize));
        bytes
    }

    /// Feed `bytes` in `chunk`-sized pieces, one per poll, with the polls in
    /// between reporting nothing.
    fn feed(
        reader: &mut FrameReader<Shape>,
        bytes: &[u8],
        chunk: usize,
    ) -> Result<Option<Frame>, Box<dyn std::error::Error + Send + Sync>> {
        let mut offset = 0;
        loop {
            let mut progressed = false;
            let result = reader.poll(Duration::from_millis(5), |target, _| {
                if progressed {
                    return Err(io::Error::from(io::ErrorKind::WouldBlock).into());
                }
                let count = chunk.min(bytes.len() - offset).min(target.len());
                target[..count].copy_from_slice(&bytes[offset..offset + count]);
                offset += count;
                progressed = true;
                Ok(count)
            })?;
            if let Some(header) = result {
                return Ok(Some(header));
            }
        }
    }

    #[test]
    fn a_frame_split_across_polls_keeps_its_boundary() {
        for chunk in [1, 2, 3, 5, 7, 9] {
            let bytes = frame(6);
            let header = feed(&mut FrameReader::new(Duration::from_secs(5)), &bytes, chunk)
                .unwrap()
                .expect("frame was lost across polls");
            assert_eq!(header.len(), 6, "chunk size {chunk}");
            assert_eq!(header.payload, vec![7; 6]);
        }
    }

    #[test]
    fn consecutive_frames_do_not_bleed_into_each_other() {
        let bytes = [frame(2), frame(5)].concat();
        let mut reader = FrameReader::<Shape>::new(Duration::from_secs(5));
        assert_eq!(feed(&mut reader, &bytes[..7], 2).unwrap().unwrap().len(), 2);
        assert_eq!(feed(&mut reader, &bytes[7..], 2).unwrap().unwrap().len(), 5);
    }

    #[test]
    fn an_expired_partial_frame_terminates_instead_of_trickling() {
        let bytes = frame(6);
        let mut reader = FrameReader::<Shape>::new(Duration::from_millis(50));
        // Exactly one byte, then nothing: the frame's lifetime starts there and
        // nothing after it may renew that lifetime.
        let mut first = true;
        reader
            .poll(Duration::from_secs(5), |target, _| {
                if !first {
                    return Err(io::Error::from(io::ErrorKind::WouldBlock).into());
                }
                first = false;
                target[..1].copy_from_slice(&bytes[..1]);
                Ok(1)
            })
            .unwrap();
        assert!(reader.partial());
        std::thread::sleep(Duration::from_millis(60));
        let error = reader
            .poll(Duration::from_millis(5), |_, _| {
                panic!("an expired partial frame must not read again")
            })
            .unwrap_err();
        assert!(error.to_string().contains("deadline expired"), "{error}");
    }

    /// A count that cannot be honoured must be rejected before anything is
    /// allocated or read, not truncated and not honoured.
    #[test]
    fn an_over_ceiling_count_is_rejected_before_payload_io() {
        let mut reader = FrameReader::<Shape>::new(Duration::from_secs(5));
        let mut header = b"TST1".to_vec();
        header.push(200);
        let error = reader
            .poll(Duration::from_millis(5), |target, _| {
                target[..5].copy_from_slice(&header);
                Ok(5)
            })
            .unwrap_err();
        assert!(error.to_string().contains("ceiling"), "{error}");
    }

    #[test]
    fn a_closed_peer_is_an_error_not_a_short_frame() {
        let mut reader = FrameReader::<Shape>::new(Duration::from_secs(5));
        let error = reader
            .poll(Duration::from_millis(5), |_, _| Ok(0))
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn an_interrupted_read_is_retried_without_losing_progress() {
        let bytes = frame(3);
        let mut reader = FrameReader::<Shape>::new(Duration::from_secs(5));
        let mut offset = 0;
        let mut interrupted = false;
        let header = reader
            .poll(Duration::from_secs(5), |target, _| {
                if !interrupted {
                    interrupted = true;
                    return Err(io::Error::from(io::ErrorKind::Interrupted).into());
                }
                let count = 2.min(bytes.len() - offset).min(target.len());
                target[..count].copy_from_slice(&bytes[offset..offset + count]);
                offset += count;
                Ok(count)
            })
            .unwrap();
        assert_eq!(header.unwrap().len(), 3);
    }
}
