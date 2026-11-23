"""Regression: binary length-prefixed TCP framing capture across real processes.

Runs hydra-proxy + hydra-core + a loopback echo upstream and activates a
declarative length-prefix framing profile through the public HTTP API
(`PUT /api/v1/protocol-profiles/...` then `POST /api/v1/protocols/.../activate`).

Covered:
  * a frame whose 2-byte length header and payload are delivered across
    separate TCP writes is decoded exactly once (split framing);
  * many frames coalesced into a single write are decoded as separate
    messages, with no merging and no duplication;
  * forwarded bytes are byte-for-byte identical in both directions (the echo
    upstream returns exactly what it received);
  * the Core history API returns the same bytes for both directions and the
    Core message API returns exactly the logical frames — one message per
    frame whose `fields.payload` is the whole frame including its length
    header (`FramingDecoder::feed` reports the complete decoded buffer, and
    `includes_header` changes only the length arithmetic, not what is exposed);
  * before activation the same service yields no decoded frame messages, so
    the positive assertions are attributable to the activation.

Runtime limit honoured, not asserted as a defect: `FramingDecoder::feed`
(crates/hydra-protocol-runtime/src/framing.rs) disables inspection when one
read produces more than 32 completed frames. This test therefore never puts
more than 32 frames into one write; the 40-frame stream is separated by an
echo barrier so no single proxy read can exceed that cap.

Run after `cargo build --workspace`:
    python3 tests/e2e/tcp_framing_capture.py
"""
import http.client
import json
import pathlib
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from relay import BIN, port

PROFILE = 'captest'
VERSION = '1'
MAX_FRAME = 64
SPLIT_FRAMES = 8
COALESCED_FRAMES = 32
TOTAL_FRAMES = SPLIT_FRAMES + COALESCED_FRAMES


def payloads():
    """Binary payloads; one is empty and one starts with bytes that look like a length header."""
    out = []
    for i in range(TOTAL_FRAMES):
        if i == 3:
            out.append(b'')
        elif i == 11:
            out.append(b'\x00\x40' + bytes(range(8)))
        else:
            out.append(bytes([i, 0xFF, 0x00, 0x0D, 0x0A]) + bytes([0x41 + i % 26]) * (i % 5))
    return out


def frame(payload):
    return len(payload).to_bytes(2, 'big') + payload


def read_exact(sock, count):
    data = b''
    while len(data) < count:
        chunk = sock.recv(count - len(data))
        if not chunk:
            raise AssertionError(f'connection closed after {len(data)}/{count} bytes')
        data += chunk
    return data


class TcpFramingCapture(unittest.TestCase):
    def test_length_prefixed_frames_split_and_coalesced(self):
        stop = threading.Event()
        upstream = socket.socket()
        upstream.bind(('127.0.0.1', 0))
        upstream.listen()
        upstream.settimeout(0.2)

        def echo(conn):
            with conn:
                while data := conn.recv(16384):
                    conn.sendall(data)

        def accept_loop():
            while not stop.is_set():
                try:
                    conn, _ = upstream.accept()
                except socket.timeout:
                    continue
                except OSError:
                    return
                threading.Thread(target=echo, args=(conn,), daemon=True).start()

        threading.Thread(target=accept_loop, daemon=True).start()
        processes = []
        with tempfile.TemporaryDirectory(prefix='hydra-framing-') as directory:
            tmp = pathlib.Path(directory)
            proxy_port, http_port = port(), port()
            sock = str(tmp / 'proxy.sock')
            origin = 'https://hydra.test'
            config = {'socket': sock, 'state_file': str(tmp / 'saved.json'),
                      'snapshot': {'revision': 0, 'services': []}}
            (tmp / 'proxy.json').write_text(json.dumps(config))
            log = open(tmp / 'logs', 'w+')
            args = [str(BIN / 'hydra-core'), '--database', str(tmp / 'core.db'),
                    '--key', str(tmp / 'jwt.pem'), '--socket', sock]

            def request(method, path, payload=None, token=None, key=None):
                c = http.client.HTTPConnection('127.0.0.1', http_port, timeout=5)
                headers = {'Origin': origin}
                if key:
                    headers['Idempotency-Key'] = key
                if token:
                    headers['Authorization'] = 'Bearer ' + token
                if payload is not None:
                    headers['Content-Type'] = 'application/json'
                c.request(method, '/api/v1' + path,
                          json.dumps(payload) if payload is not None else None, headers)
                r = c.getresponse()
                raw = r.read()
                status = r.status
                c.close()
                return status, json.loads(raw) if raw else None

            def good(method, path, payload=None, token=None, key=None):
                status, body = request(method, path, payload, token, key)
                self.assertIn(status, [200, 201, 202, 204], (status, body))
                return body

            def wait_for(fn, timeout=8.0):
                deadline = time.monotonic() + timeout
                while time.monotonic() < deadline:
                    value = fn()
                    if value:
                        return value
                    time.sleep(.05)
                return None

            def flow_by_port(client_port):
                page = good('GET', f'/traffic/flows?service_id={sid}&newest_first=true&limit=50', token=alice)
                return next((f for f in page['items']
                             if (f.get('client') or {}).get('port') == client_port), None)

            def messages(flow_id):
                return good('GET', f'/traffic/flows/{flow_id}/messages?limit=100', token=alice)['items']

            def history(flow_id):
                try:
                    status, body = request('GET', f'/traffic/flows/{flow_id}/history', token=alice)
                except (OSError, http.client.HTTPException):
                    return None
                return body if status == 200 else None

            try:
                subprocess.run(args + ['create', 'user', 'alice', 'test-password-1234'],
                               check=True, stdout=log, stderr=log, timeout=10)
                processes.append(subprocess.Popen(
                    [str(BIN / 'hydra-proxy'), '--config', str(tmp / 'proxy.json')],
                    stdout=log, stderr=log))
                processes.append(subprocess.Popen(
                    args + ['serve', '--listen', f'127.0.0.1:{http_port}', '--origin', origin],
                    stdout=log, stderr=log))
                for _ in range(200):
                    try:
                        if request('GET', '/services')[0] == 401:
                            break
                    except (OSError, http.client.HTTPException):
                        pass
                    time.sleep(.05)
                else:
                    log.seek(0)
                    self.fail('Core HTTP did not become ready:\n' + log.read())
                alice = good('POST', '/auth/login',
                             {'username': 'alice', 'password': 'test-password-1234'})['access_token']

                created = good('POST', '/services', {
                    'expected_revision': 0,
                    'transport': 'tcp',
                    'listen_address': f'127.0.0.1:{proxy_port}',
                    'upstream_address': f'127.0.0.1:{upstream.getsockname()[1]}',
                    'protocol_id': 'raw',
                    'enabled': True,
                    'retain_history': True,
                }, alice)
                sid = created['id']
                self.assertTrue(wait_for(
                    lambda: good('GET', '/system/proxy', token=alice)['applied_services_revision'] == 1),
                    'Proxy did not apply the service snapshot')

                # Negative control: with protocol_id=raw and no activation, the
                # same framed bytes must not produce decoded frame messages.
                probe = socket.create_connection(('127.0.0.1', proxy_port), timeout=5)
                probe.settimeout(10)
                probe_port = probe.getsockname()[1]
                probe_stream = frame(b'pre-activation')
                probe.sendall(probe_stream)
                self.assertEqual(read_exact(probe, len(probe_stream)), probe_stream)
                probe.shutdown(socket.SHUT_WR)
                try:
                    while probe.recv(4096):
                        pass
                except ConnectionResetError:
                    pass
                probe.close()
                probe_flow = wait_for(lambda: flow_by_port(probe_port))
                self.assertIsNotNone(probe_flow, 'Core did not index the pre-activation flow')
                probe_items = wait_for(lambda: messages(probe_flow['flow_id']) or None)
                self.assertIsNotNone(probe_items, 'pre-activation flow produced no messages at close')
                self.assertNotIn(PROFILE, {item['protocol_id'] for item in probe_items},
                                 'framing decoder produced messages before activation')
                self.assertEqual({item['protocol_id'] for item in probe_items}, {'transfer'},
                                 'raw service must only report the byte summary')

                profile = good('PUT', f'/protocol-profiles/{PROFILE}/{VERSION}', {
                    'framing': {'kind': 'length_prefix', 'offset': 0, 'width': 2,
                                'little_endian': False, 'includes_header': False},
                    'max_frame': MAX_FRAME,
                }, alice)
                self.assertEqual(profile['state'], 'installed')
                activated = good('POST', f'/protocols/{PROFILE}/{VERSION}/activate',
                                 {'service_id': sid}, alice, 'tcp-framing-capture-1')
                self.assertTrue(activated['applied'], activated)
                listed = good('GET', '/protocols', token=alice)
                self.assertIn((sid, PROFILE, VERSION, True),
                              {(a['service_id'], a['id'], a['version'], a['applied'])
                               for a in listed['activations']})

                frames = payloads()
                sent = b''.join(frame(p) for p in frames)
                split_bytes = sum(len(frame(p)) for p in frames[:SPLIT_FRAMES])
                self.assertGreater(split_bytes, 0)

                client = socket.create_connection(('127.0.0.1', proxy_port), timeout=5)
                client.settimeout(10)
                client_port = client.getsockname()[1]
                echoed = b''
                # Split phase: 3-byte writes with an echo barrier after each one.
                # This forces header and payload bytes of a frame into different
                # proxy reads without ever exceeding the per-read frame cap.
                # The final slice is clamped to split_bytes so it never overlaps
                # the coalesced tail (sending those bytes twice would corrupt
                # this fixture's own stream).
                for offset in range(0, split_bytes, 3):
                    piece = sent[offset:min(offset + 3, split_bytes)]
                    client.sendall(piece)
                    got = read_exact(client, len(piece))
                    self.assertEqual(got, piece, f'proxy altered split write at offset {offset}')
                    echoed += got
                # Coalesced phase: the remaining frames in a single write.
                tail_stream = sent[split_bytes:]
                self.assertGreater(len(tail_stream), 0)
                client.sendall(tail_stream)
                got = read_exact(client, len(tail_stream))
                self.assertEqual(got, tail_stream, 'proxy altered coalesced frames')
                echoed += got
                client.shutdown(socket.SHUT_WR)
                leftover = b''
                try:
                    while chunk := client.recv(4096):
                        leftover += chunk
                except ConnectionResetError:
                    pass
                client.close()
                self.assertEqual(echoed, sent, 'forwarded client bytes were not byte-for-byte intact')
                self.assertEqual(leftover, b'', 'proxy or fixture appended unexpected bytes')

                flow = wait_for(lambda: flow_by_port(client_port))
                self.assertIsNotNone(flow, 'Core did not index the framed flow')
                flow_id = flow['flow_id']

                # The framing decoder stores the entire consumed buffer as the
                # `payload` field, so a length-prefix message carries its 2-byte
                # header too: crates/hydra-protocol-runtime/src/framing.rs:88-96
                # (`Value::Bytes(std::mem::take(buffer))`), consistent with the
                # runtime tests at framing.rs:145-148 and framing.rs:167-170.
                expected = {1: [list(frame(p)) for p in frames],
                            2: [list(frame(p)) for p in frames]}

                def decoded():
                    items = messages(flow_id)
                    return items if len(items) >= 2 * TOTAL_FRAMES else None

                items = wait_for(decoded, timeout=10)
                self.assertIsNotNone(
                    items, f'expected {2 * TOTAL_FRAMES} decoded messages, got '
                           f'{len(messages(flow_id))}')
                items = sorted(items, key=lambda item: item['stream_sequence'])
                by_direction = {1: [], 2: []}
                for item in items:
                    self.assertEqual(item['protocol_id'], PROFILE, item)
                    self.assertEqual(item['message_type'], 'frame', item)
                    by_direction[item['direction']].append(item)
                for direction in (1, 2):
                    self.assertEqual(len(by_direction[direction]), TOTAL_FRAMES,
                                     f'direction {direction} frame count changed')
                    self.assertEqual(
                        [item['fields']['payload']['value'] for item in by_direction[direction]],
                        expected[direction],
                        f'direction {direction} decoded frames differ from the logical frames')
                    self.assertTrue(all(item['fields']['payload']['type'] == 'bytes'
                                        for item in by_direction[direction]))

                saved = wait_for(lambda: history(flow_id))
                self.assertIsNotNone(saved, 'Proxy did not expose the closed flow history')
                self.assertFalse(saved['history_truncated'])
                self.assertFalse(saved['truncated_client_to_server'])
                self.assertFalse(saved['truncated_server_to_client'])
                captured = {1: b'', 2: b''}
                for segment in saved['segments']:
                    captured[segment['direction']] += bytes(segment['data'])
                self.assertEqual(captured[1], sent,
                                 'client-to-server history is not byte-for-byte intact')
                self.assertEqual(captured[2], sent,
                                 'server-to-client history is not byte-for-byte intact')
                self.assertEqual(saved['bytes_client_to_server'], len(sent))
                self.assertEqual(saved['bytes_server_to_client'], len(sent))
            finally:
                for process in processes:
                    if process.poll() is None:
                        process.terminate()
                    process.wait(timeout=10)
                stop.set()
                upstream.close()
                log.close()


if __name__ == '__main__':
    unittest.main()
