//! Ordered bounded TCP read chunks. Direction tails keep their original byte limits.
use super::{Buffer, Datagram, MAX_DATAGRAMS, MAX_DIRECTION_BYTES, now_ms};
use hydra_protocol_sdk::Direction;
impl Buffer {
    pub(super) fn record_tcp(&mut self, direction: Direction, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let index = usize::from(direction == Direction::ServerToClient);
        self.datagram_totals[index] = self.datagram_totals[index].saturating_add(1);
        let sequence = self.datagram_totals[0].saturating_add(self.datagram_totals[1]);
        if bytes.len() > MAX_DIRECTION_BYTES {
            self.mark_truncated(index);
            self.dropped[index] =
                self.dropped[index].saturating_add((bytes.len() - MAX_DIRECTION_BYTES) as u64);
        }
        let bytes = &bytes[bytes.len().saturating_sub(MAX_DIRECTION_BYTES)..];
        let mut overflow = self.datagram_bytes[index]
            .saturating_add(bytes.len())
            .saturating_sub(MAX_DIRECTION_BYTES);
        while overflow > 0 {
            let Some(at) = self
                .datagrams
                .iter()
                .position(|chunk| usize::from(chunk.direction == 2) == index)
            else {
                break;
            };
            let length = self.datagrams[at].data.len();
            let remove = overflow.min(length);
            if remove == length {
                self.datagrams.remove(at);
            } else {
                self.datagrams[at].data.drain(..remove);
            }
            self.datagram_bytes[index] -= remove;
            overflow -= remove;
            self.dropped[index] = self.dropped[index].saturating_add(remove as u64);
            self.mark_truncated(index);
        }
        while self.datagrams.len() >= MAX_DATAGRAMS {
            if let Some(old) = self.datagrams.pop_front() {
                let side = usize::from(old.direction == 2);
                self.datagram_bytes[side] -= old.data.len();
                self.dropped[side] = self.dropped[side].saturating_add(old.data.len() as u64);
                self.mark_truncated(side);
            }
        }
        self.datagram_bytes[index] += bytes.len();
        self.datagrams.push_back(Datagram {
            direction: if index == 0 { 1 } else { 2 },
            occurred_at_ms: now_ms(),
            sequence,
            data: bytes.to_vec(),
        });
    }
    fn mark_truncated(&mut self, index: usize) {
        if index == 0 {
            self.truncated_client = true
        } else {
            self.truncated_server = true
        }
    }
    pub(super) fn tcp_payloads(&self) -> (Vec<u8>, Vec<u8>) {
        if self.udp {
            return (Vec::new(), Vec::new());
        }
        let mut client = Vec::with_capacity(self.datagram_bytes[0]);
        let mut server = Vec::with_capacity(self.datagram_bytes[1]);
        for chunk in &self.datagrams {
            if chunk.direction == 1 {
                client.extend_from_slice(&chunk.data)
            } else {
                server.extend_from_slice(&chunk.data)
            }
        }
        (client, server)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn direction_tail_trims_bytes_without_grouping_the_other_direction() {
        let mut b = Buffer::default();
        b.record_tcp(
            Direction::ServerToClient,
            &vec![b'x'; MAX_DIRECTION_BYTES - 2],
        );
        b.record_tcp(Direction::ClientToServer, b"a");
        b.record_tcp(Direction::ServerToClient, b"BCDE");
        let (client, server) = b.tcp_payloads();
        assert_eq!(client, b"a");
        assert_eq!(server.len(), MAX_DIRECTION_BYTES);
        assert!(server.ends_with(b"BCDE"));
        assert_eq!(
            b.datagrams.iter().map(|c| c.direction).collect::<Vec<_>>(),
            vec![2, 1, 2]
        );
        assert!(b.truncated_server);
        assert!(!b.truncated_client);
        for _ in 0..MAX_DATAGRAMS + 1 {
            b.record_tcp(Direction::ClientToServer, b"b")
        }
        assert!(b.datagrams.len() <= MAX_DATAGRAMS);
        assert!(b.truncated_client);
    }
}
