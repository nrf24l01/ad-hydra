"""End-to-end regression: the Streams workspace over one WebSocket.

Boots the real two-process harness (``hydra-proxy`` + ``hydra-core``) plus a
loopback UTF-8 echo upstream, then drives the page-scoped socket
(``GET /api/v1/traffic/events/socket``, subprotocol ``hydra.traffic.v1``)
exactly as ``TrafficPage`` does, per the frozen contract in
``docs/superpowers/specs/2026-11-23-streams-workspace-design.md`` §3-§5.

Proven here (protocol level, no browser):
  * the upgrade enforces ``Origin`` (wrong origin -> 403 before upgrade) and
    echoes ``Sec-WebSocket-Protocol: hydra.traffic.v1`` with a correct
    ``Sec-WebSocket-Accept``;
  * the first frame must be ``auth`` and a bad token is rejected with 4401;
  * ``subscribe`` returns a 100-item ``snapshot``; ``page`` returns older
    ``flows_page`` rows over the same socket, so the list is WS-only;
  * a stream opened after ``subscribe`` is pushed as a live ``flow`` frame,
    and bytes on the selected flow are pushed as live ``message`` frames
    filtered to that flow (no polling);
  * two services are populated with more streams than fit in the first page;
    an old target stream is *not* in the snapshot but is reachable by
    ``page`` and by ``search`` (default scope ``both``, all services), which
    proves search is not limited to the loaded list;
  * search is a literal, case-insensitive substring: ``needle.one`` matches
    the literal dot and a ``needleXone`` decoy does not; a query containing a
    backslash is literal too;
  * ``flag_spans`` on ``messages_page`` items and ``search_page`` items are
    UTF-16 code-unit offsets relative to the returned text/snippet and are
    correct with a 4-byte emoji before the flag (code-point and byte offsets
    would slice wrongly);
  * selecting a still-open flow with ``active_bytes`` yields live
    ``active_bytes`` frames whose segments assemble byte-for-byte to the
    forwarded stream, including a 4-byte binary prefix and UTF-8 Cyrillic,
    then append only new chunk sequences (no duplicated assembly);
  * the 64 KiB per-direction tail is bounded: after an 80 KiB burst the next
    snapshot has ``reset:true``, ``truncated_client_to_server`` and a
    ``client_head_dropped`` count, and the retained bytes are exactly the
    tail of the stream;
  * the client contract's ``pause``/``resume`` are honored around *every*
    phase that blocks the single-threaded reader: the 80 KiB echo round trip,
    the auth refresh, and the legacy HTTP history probe with its audit wait.
    Pausing stops live frames and the active_bytes poll, so the documented
    4 MiB / 64-frame outbound queue is never asked to buffer frames the reader
    cannot drain (a genuine 1013 ``slow consumer``); each resume awaits the
    newer ``snapshot`` generation and re-selects the same flow before any
    further byte assertion, so the queue bounds themselves are unchanged;
  * each ``select`` request (initial, post-burst, post-audit) writes exactly
    one ``traffic.history.subscribe`` audit row carrying that frame's
    ``request_id`` and does *not* call the legacy HTTP history route (zero
    ``traffic.history.read`` rows until the explicit legacy probe);
  * the legacy HTTP history route still answers 409 for an active flow
    (expected, not an error) and 200 with the persisted tail after close; its
    ``traffic.history.read`` / ``traffic.history.result`` audit rows share one
    request id (audit correlation);
  * closing the flow delivers the final persisted history as an
    ``active_bytes`` frame with ``snapshot.active=false`` and no duplicated
    chunk sequence;
  * reconnect with a fresh login token and in-band re-auth both re-snapshot.

Deliberately not covered: the Vue rendering. This project has no Playwright
dependency (``frontend/package.json`` has no ``@playwright/test`` and
``node_modules`` contains no browser driver); ``tests/e2e/browser.py`` is a
separate, hand-rolled CDP harness owned by another test and is not imported
here. Per the task rule, no browser UI verification is fabricated: the
``<mark>`` rendering is asserted at the protocol boundary (server-computed
UTF-16 ``flag_spans``) and the browser layer is reported as missing.

The WebSocket client below is standard library only (``socket`` + ``sha1`` +
masking), because the test runner's Python is not guaranteed to have
``websockets`` installed. It implements the bounded subset of RFC 6455 needed
for text frames, ping/pong, close and the handshake; frames larger than 2 MiB
or inbound binary are rejected rather than buffered.

Run after ``cargo build --workspace``:
    python3 tests/e2e/streams_workspace.py
"""
import base64
import hashlib
import http.client
import json
import os
import pathlib
import socket
import sqlite3
import struct
import subprocess
import tempfile
import threading
import time
import unittest
import uuid

from relay import BIN, port

ORIGIN = 'https://hydra.test'
SUBPROTOCOL = 'hydra.traffic.v1'
WS_PATH = '/api/v1/traffic/events/socket'
WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'

FIRST_PAGE = 100
FILLERS = 104
MAX_DIRECTION_BYTES = 64 * 1024
MAX_INBOUND_FRAME = 2 * 1024 * 1024

FLAG = 'FLAG{abc123}'
FLAG_PATTERN = r'FLAG\{[a-z0-9]+\}'
EMOJI = '\U0001f6a9'            # 4-byte UTF-8, 2 UTF-16 code units
MARKER = 'needle.one'           # literal dot; regex would also match needleXone
DECOY = 'needleXone'
BACKSLASH_QUERY = r'C:\hydra\old'
BINARY_PREFIX = b'\x00\xff\x01\xfe'
CYRILLIC = '\u043f\u0440\u0438\u0432\u0435\u0442-\u0433\u0438\u0434\u0440\u0430-\u03bb'


def read_exact(sock, count):
    data = b''
    while len(data) < count:
        chunk = sock.recv(min(65536, count - len(data)))
        if not chunk:
            raise AssertionError(f'connection closed after {len(data)}/{count} bytes')
        data += chunk
    return data


def ws_accept(key):
    digest = hashlib.sha1((key + WS_GUID).encode()).digest()
    return base64.b64encode(digest).decode()


class WSClosed(Exception):
    def __init__(self, code, reason=''):
        super().__init__(f'websocket closed {code} {reason!r}')
        self.code = code
        self.reason = reason


class WebSocket:
    """Minimal RFC 6455 client: masked text frames, ping/pong, close."""

    def __init__(self, http_port, origin=ORIGIN, timeout=10.0):
        self.sock = socket.create_connection(('127.0.0.1', http_port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.pending = b''
        key = base64.b64encode(os.urandom(16)).decode()
        request = (
            f'GET {WS_PATH} HTTP/1.1\r\n'
            f'Host: 127.0.0.1:{http_port}\r\n'
            'Upgrade: websocket\r\n'
            'Connection: Upgrade\r\n'
            f'Sec-WebSocket-Key: {key}\r\n'
            'Sec-WebSocket-Version: 13\r\n'
            f'Sec-WebSocket-Protocol: {SUBPROTOCOL}\r\n'
            f'Origin: {origin}\r\n\r\n')
        self.sock.sendall(request.encode())
        head = self._read_until_headers()
        status_line, _, rest = head.partition(b'\r\n')
        parts = status_line.decode('latin-1').split(' ', 2)
        self.status = int(parts[1]) if len(parts) > 1 else 0
        self.headers = {}
        for line in rest.split(b'\r\n'):
            if b':' in line:
                name, value = line.split(b':', 1)
                self.headers[name.strip().lower().decode('latin-1')] = value.strip().decode('latin-1')
        self.pending = rest.split(b'\r\n\r\n', 1)[1] if b'\r\n\r\n' in rest else b''
        if self.status != 101:
            raise AssertionError(
                f'WebSocket upgrade returned {self.status}, expected 101: '
                f'{head[:400]!r}')
        expected = ws_accept(key)
        actual = self.headers.get('sec-websocket-accept')
        if actual != expected:
            raise AssertionError(
                f'Sec-WebSocket-Accept {actual!r} != {expected!r} (wrong handshake)')
        if self.headers.get('sec-websocket-protocol') != SUBPROTOCOL:
            raise AssertionError(
                'server did not echo the negotiated subprotocol: '
                f'{self.headers.get("sec-websocket-protocol")!r}')

    def _read_until_headers(self):
        data = b''
        while b'\r\n\r\n' not in data:
            chunk = self.sock.recv(1)
            if not chunk:
                raise AssertionError(f'upgrade closed after {data!r}')
            data += chunk
            if len(data) > 65536:
                raise AssertionError('upgrade response headers exceeded 64 KiB')
        return data

    def _recv_exact(self, count):
        if self.pending:
            data, self.pending = self.pending[:count], self.pending[count:]
            if len(data) == count:
                return data
            return data + read_exact(self.sock, count - len(data))
        return read_exact(self.sock, count)

    def _send_frame(self, opcode, payload):
        header = bytes([0x80 | opcode])
        length = len(payload)
        if length < 126:
            header += bytes([0x80 | length])
        elif length < 65536:
            header += bytes([0x80 | 126]) + struct.pack('!H', length)
        else:
            header += bytes([0x80 | 127]) + struct.pack('!Q', length)
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(header + mask + masked)

    def send_text(self, value):
        self._send_frame(0x1, value.encode('utf-8'))

    def send_json(self, value):
        self.send_text(json.dumps(value))

    def recv_frame(self):
        first, second = self._recv_exact(2)
        fin = bool(first & 0x80)
        opcode = first & 0x0F
        masked = bool(second & 0x80)
        length = second & 0x7F
        if length == 126:
            length = struct.unpack('!H', self._recv_exact(2))[0]
        elif length == 127:
            length = struct.unpack('!Q', self._recv_exact(8))[0]
        if masked:
            raise AssertionError('server frame must not be masked')
        if length > MAX_INBOUND_FRAME:
            raise AssertionError(f'server frame of {length} bytes exceeds the 2 MiB bound')
        payload = self._recv_exact(length) if length else b''
        if opcode == 0x9:  # ping
            self._send_frame(0xA, payload)
            return self.recv_frame()
        if opcode == 0xA:  # pong
            return self.recv_frame()
        if opcode == 0x8:  # close
            code = struct.unpack('!H', payload[:2])[0] if len(payload) >= 2 else 1005
            raise WSClosed(code, payload[2:].decode('utf-8', 'replace'))
        if opcode not in (0x0, 0x1):
            raise AssertionError(f'unexpected server opcode {opcode}')
        if not fin:
            raise AssertionError('fragmented server frames are not part of this contract')
        try:
            return json.loads(payload.decode('utf-8'))
        except (UnicodeDecodeError, ValueError) as exc:
            raise AssertionError(f'server sent a non-JSON text frame: {exc}') from exc

    def recv_json(self, timeout=3.0):
        self.sock.settimeout(timeout)
        try:
            return self.recv_frame()
        except socket.timeout:
            return None

    def close(self, code=1000):
        try:
            self._send_frame(0x8, struct.pack('!H', code))
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


def expect_upgrade_status(http_port, origin):
    """Raw upgrade attempt; returns the HTTP status without opening a socket."""
    sock = socket.create_connection(('127.0.0.1', http_port), timeout=5)
    key = base64.b64encode(os.urandom(16)).decode()
    sock.sendall((
        f'GET {WS_PATH} HTTP/1.1\r\nHost: 127.0.0.1:{http_port}\r\n'
        'Upgrade: websocket\r\nConnection: Upgrade\r\n'
        f'Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n'
        f'Sec-WebSocket-Protocol: {SUBPROTOCOL}\r\nOrigin: {origin}\r\n\r\n'
    ).encode())
    try:
        line = b''
        while b'\r\n' not in line:
            chunk = sock.recv(1)
            if not chunk:
                break
            line += chunk
        return int(line.decode('latin-1').split(' ')[1])
    finally:
        sock.close()


def utf16_slice(text, start, end):
    raw = text.encode('utf-16-le')
    return raw[start * 2:end * 2].decode('utf-16-le')


def spans_of(item, key):
    out = []
    for span in item.get(key) or []:
        if isinstance(span, dict):
            out.append((int(span['start']), int(span['end'])))
        elif isinstance(span, (list, tuple)) and len(span) == 2:
            out.append((int(span[0]), int(span[1])))
        else:
            raise AssertionError(f'{key} has an unsupported span shape: {span!r}')
    return out


def message_text(item):
    """The string flag_spans are relative to, across the documented names.

    The frozen contract calls it the item's returned ``text``/``snippet``; the
    frontend work adds ``display_text``. Accept all three and report which one
    was used so a span mismatch is diagnosable.
    """
    for key in ('display_text', 'text', 'snippet'):
        value = item.get(key)
        if isinstance(value, str):
            return value, key
    fields = item.get('fields') or {}
    for key in ('utf8.text', 'text'):
        value = fields.get(key)
        if isinstance(value, dict):
            value = value.get('value')
        if isinstance(value, str):
            return value, 'fields.' + key
    return None, None


class ByteStreams:
    """Client-side assembly per the contract: sequence-keyed, reset replaces."""

    def __init__(self):
        self.seqs = {1: {}, 2: {}}
        self.resets = []

    def apply(self, frame):
        snapshot = frame.get('snapshot')
        if not isinstance(snapshot, dict):
            raise AssertionError(f'active_bytes frame has no snapshot: {frame}')
        reset = bool(snapshot.get('reset', True))
        self.resets.append(reset)
        for direction in (1, 2):
            if reset:
                self.seqs[direction] = {}
            for segment in snapshot.get('segments') or []:
                if int(segment['direction']) != direction:
                    continue
                sequence = int(segment['sequence'])
                data = bytes(segment['data'])
                previous = self.seqs[direction].get(sequence)
                if previous is not None and len(data) >= len(previous):
                    continue  # identical or a superseded shorter head chunk
                self.seqs[direction][sequence] = data
        return snapshot

    def assembled(self, direction):
        return b''.join(self.seqs[direction][key] for key in sorted(self.seqs[direction]))


class StreamsWorkspace(unittest.TestCase):
    def test_ws_page_live_search_flags_active_and_closed_history(self):
        stop = threading.Event()
        upstream = socket.socket()
        upstream.bind(('127.0.0.1', 0))
        upstream.listen()
        upstream.settimeout(0.2)

        def echo(conn):
            with conn:
                while True:
                    try:
                        data = conn.recv(65536)
                    except OSError:
                        return
                    if not data:
                        return
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
        ws = None
        active_client = None
        with tempfile.TemporaryDirectory(prefix='hydra-streams-') as directory:
            tmp = pathlib.Path(directory)
            bulk_port, archive_port, http_port = port(), port(), port()
            sock = str(tmp / 'proxy.sock')
            config = {'socket': sock, 'state_file': str(tmp / 'saved.json'),
                      'snapshot': {'revision': 0, 'services': []},
                      'flag_pattern': FLAG_PATTERN}
            (tmp / 'proxy.json').write_text(json.dumps(config))
            operations = tmp / 'operations.json'
            operations.write_text(json.dumps({'flag_pattern': FLAG_PATTERN,
                                              'auto_submit_captured': False}))
            operations.chmod(0o600)
            log = open(tmp / 'logs', 'w+')
            database = tmp / 'core.db'
            args = [str(BIN / 'hydra-core'), '--database', str(database),
                    '--key', str(tmp / 'jwt.pem'), '--socket', sock]

            def request(method, path, body=None, token=None, key=None):
                c = http.client.HTTPConnection('127.0.0.1', http_port, timeout=10)
                headers = {'Origin': ORIGIN}
                if key:
                    headers['Idempotency-Key'] = key
                if token:
                    headers['Authorization'] = 'Bearer ' + token
                if body is not None:
                    headers['Content-Type'] = 'application/json'
                c.request(method, '/api/v1' + path,
                          json.dumps(body) if body is not None else None, headers)
                r = c.getresponse()
                raw = r.read()
                status = r.status
                c.close()
                try:
                    parsed = json.loads(raw) if raw else None
                except ValueError:
                    parsed = None
                return status, parsed, raw

            def good(method, path, body=None, token=None, key=None):
                status, reply, raw = request(method, path, body, token, key)
                self.assertIn(status, [200, 201, 202, 204], (status, raw[:400]))
                return reply

            def wait_for(fn, timeout=20.0):
                deadline = time.monotonic() + timeout
                while time.monotonic() < deadline:
                    value = fn()
                    if value:
                        return value
                    time.sleep(.05)
                return None

            def login_session():
                """Login and keep the refresh cookie + CSRF token.

                In-band re-auth must present a token for the *same* session; a
                fresh login would mint a new sid and the socket rejects a sid
                switch. The refresh endpoint is the only same-session renewal.
                """
                c = http.client.HTTPConnection('127.0.0.1', http_port, timeout=10)
                c.request('POST', '/api/v1/auth/login',
                          json.dumps({'username': 'alice',
                                      'password': 'test-password-1234'}),
                          {'Origin': ORIGIN, 'Content-Type': 'application/json'})
                r = c.getresponse()
                raw = r.read()
                headers = r.getheaders()
                c.close()
                body = json.loads(raw)
                refresh = None
                for name, value in headers:
                    if name.lower() == 'set-cookie' and value.startswith('hydra_refresh='):
                        refresh = value.split(';', 1)[0].split('=', 1)[1]
                self.assertIsNotNone(refresh, 'login did not set the refresh cookie')
                return body['access_token'], refresh, body['csrf_token']

            def refresh_session(session):
                _, refresh, csrf = session
                c = http.client.HTTPConnection('127.0.0.1', http_port, timeout=10)
                c.request('POST', '/api/v1/auth/refresh', None, {
                    'Origin': ORIGIN,
                    'X-CSRF-Token': csrf,
                    'Cookie': f'hydra_refresh={refresh}; hydra_csrf={csrf}',
                })
                r = c.getresponse()
                raw = r.read()
                headers = r.getheaders()
                c.close()
                self.assertEqual(r.status, 200, raw[:400])
                body = json.loads(raw)
                rotated = refresh
                for name, value in headers:
                    if name.lower() == 'set-cookie' and value.startswith('hydra_refresh='):
                        rotated = value.split(';', 1)[0].split('=', 1)[1]
                return body['access_token'], rotated, body['csrf_token']

            def flow_by_port(service_id, client_port):
                page = good('GET', f'/traffic/flows?service_id={service_id}'
                                   '&newest_first=true&limit=100', token=alice)
                return next((f for f in page['items']
                             if (f.get('client') or {}).get('port') == client_port), None)

            def exchange(payload, hold=False, listen_port=None):
                conn = socket.create_connection(
                    ('127.0.0.1', listen_port or bulk_port), timeout=10)
                conn.settimeout(30)
                client_port = conn.getsockname()[1]
                conn.sendall(payload)
                echoed = read_exact(conn, len(payload))
                if not hold:
                    conn.shutdown(socket.SHUT_WR)
                    try:
                        while conn.recv(4096):
                            pass
                    except (ConnectionResetError, socket.timeout):
                        pass
                    conn.close()
                return conn, client_port, echoed

            def audit_rows(target):
                try:
                    con = sqlite3.connect(f'file:{database}?mode=ro', uri=True, timeout=5)
                except sqlite3.Error:
                    return []
                try:
                    return con.execute(
                        'SELECT action,request_id,success FROM audit '
                        'WHERE target=? ORDER BY id', (target,)).fetchall()
                except sqlite3.Error:
                    return []
                finally:
                    con.close()

            def next_frame(predicate, timeout=20.0, context='frame'):
                deadline = time.monotonic() + timeout
                while time.monotonic() < deadline:
                    frame = ws.recv_json(timeout=2.0)
                    if frame is None:
                        continue
                    self.assertNotEqual(frame.get('type'), 'error',
                                        f'WS error while waiting for {context}: {frame}')
                    self.assertNotEqual(frame.get('type'), 'reauth_required',
                                        f'unexpected reauth while waiting for {context}: {frame}')
                    if predicate(frame):
                        return frame
                self.fail(f'timed out waiting for {context}')

            def active_frames_until(predicate, timeout=20.0, context='active_bytes'):
                deadline = time.monotonic() + timeout
                while time.monotonic() < deadline:
                    frame = ws.recv_json(timeout=2.0)
                    if frame is None:
                        continue
                    kind = frame.get('type')
                    if kind == 'error':
                        self.fail(f'WS error while waiting for {context}: {frame}')
                    if kind != 'active_bytes':
                        continue
                    if predicate(frame):
                        return frame
                self.fail(f'timed out waiting for {context}')

            try:
                subprocess.run(args + ['create', 'user', 'alice', 'test-password-1234'],
                               check=True, stdout=log, stderr=log, timeout=10)
                processes.append(subprocess.Popen(
                    [str(BIN / 'hydra-proxy'), '--config', str(tmp / 'proxy.json')],
                    stdout=log, stderr=log))
                processes.append(subprocess.Popen(
                    args + ['serve', '--listen', f'127.0.0.1:{http_port}',
                            '--origin', ORIGIN, '--operations', str(operations)],
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
                session = login_session()
                alice = session[0]

                bulk = good('POST', '/services', {
                    'expected_revision': 0,
                    'transport': 'tcp',
                    'listen_address': f'127.0.0.1:{bulk_port}',
                    'upstream_address': f'127.0.0.1:{upstream.getsockname()[1]}',
                    'protocol_id': 'utf8',
                    'enabled': True,
                    'retain_history': True,
                }, alice)
                bulk_id = bulk['id']
                archive = good('POST', '/services', {
                    'expected_revision': 1,
                    'transport': 'tcp',
                    'listen_address': f'127.0.0.1:{archive_port}',
                    'upstream_address': f'127.0.0.1:{upstream.getsockname()[1]}',
                    'protocol_id': 'utf8',
                    'enabled': True,
                    'retain_history': True,
                }, alice)
                archive_id = archive['id']
                self.assertNotEqual(bulk_id, archive_id)
                self.assertTrue(wait_for(
                    lambda: good('GET', '/system/proxy', token=alice)['applied_services_revision'] == 2),
                    'Proxy did not apply both utf8 service snapshots')

                # 1. Old search target, on the *other* service, created first so
                #    it falls outside the newest-100 snapshot.
                old_payload = (f'{EMOJI} answer {FLAG} {MARKER} {BACKSLASH_QUERY}\n'
                               .encode('utf-8'))
                old_sock = socket.create_connection(('127.0.0.1', archive_port), timeout=10)
                old_sock.settimeout(30)
                old_port = old_sock.getsockname()[1]
                old_sock.sendall(old_payload)
                self.assertEqual(read_exact(old_sock, len(old_payload)), old_payload)
                old_sock.shutdown(socket.SHUT_WR)
                try:
                    while old_sock.recv(4096):
                        pass
                except (ConnectionResetError, socket.timeout):
                    pass
                old_sock.close()
                old_flow = wait_for(lambda: flow_by_port(archive_id, old_port))
                self.assertIsNotNone(old_flow, 'Core did not index the old archive flow')
                old_id = old_flow['flow_id']
                self.assertEqual(old_flow['service_id'], archive_id)

                # 2. More than one page of newer bulk flows.
                filler_port = None
                for index in range(FILLERS):
                    payload = f'filler {index:03d} plain ascii line\n'.encode()
                    conn, client_port, echoed = exchange(payload)
                    self.assertEqual(echoed, payload)
                    filler_port = client_port
                self.assertTrue(wait_for(lambda: flow_by_port(bulk_id, filler_port)),
                                'Core did not index the last filler flow')

                # 3. Decoy (literal-dot negative control) on the archive service,
                #    newest of the archive flows, so it is inside the first page.
                decoy_payload = f'{DECOY} not a literal match\n'.encode()
                decoy_sock, decoy_port, echoed = exchange(
                    decoy_payload, listen_port=archive_port)
                self.assertEqual(echoed, decoy_payload)
                decoy_flow = wait_for(lambda: flow_by_port(archive_id, decoy_port))
                self.assertIsNotNone(decoy_flow)
                decoy_id = decoy_flow['flow_id']

                # 4. Still-open flow that will be selected with active_bytes.
                initial = BINARY_PREFIX + CYRILLIC.encode('utf-8')
                active_client, active_port, echoed = exchange(initial, hold=True)
                self.assertEqual(echoed, initial)
                active_flow = wait_for(lambda: flow_by_port(bulk_id, active_port))
                self.assertIsNotNone(active_flow, 'Core did not index the open flow')
                active_id = active_flow['flow_id']
                self.assertEqual(active_flow['service_id'], bulk_id)

                # The whole setup can outlive a 60 s access token; renew the
                # same session before the socket authenticates.
                session = refresh_session(session)
                alice = session[0]

                # Upgrade must enforce Origin before the upgrade happens.
                self.assertEqual(expect_upgrade_status(http_port, 'https://evil.test'), 403,
                                 'WebSocket upgrade did not reject a foreign Origin')

                ws = WebSocket(http_port)
                self.assertEqual(ws.headers.get('sec-websocket-protocol'), SUBPROTOCOL)

                # Bad token: 4401 (an error frame may precede the close).
                bad = WebSocket(http_port)
                bad.send_json({'type': 'auth', 'request_id': str(uuid.uuid4()),
                               'token': 'not-a-jwt'})
                closed = None
                deadline = time.monotonic() + 8
                while time.monotonic() < deadline and closed is None:
                    try:
                        frame = bad.recv_json(timeout=2.0)
                    except WSClosed as exc:
                        closed = exc.code
                        break
                    if frame is not None:
                        self.assertEqual(frame.get('type'), 'error', frame)
                bad.close()
                self.assertEqual(closed, 4401,
                                 f'invalid auth must close 4401, got {closed}')

                auth_request = str(uuid.uuid4())
                ws.send_json({'type': 'auth', 'request_id': auth_request, 'token': alice})
                ready = next_frame(lambda f: f.get('type') == 'ready', context='ready')
                self.assertEqual(ready.get('protocol'), 1)
                self.assertTrue(ready.get('connection_id'))
                self.assertTrue(ready.get('actor'))
                self.assertGreater(int(ready.get('expires_in_ms', 0)), 0)

                subscribe_request = str(uuid.uuid4())
                ws.send_json({'type': 'subscribe', 'request_id': subscribe_request,
                              'query': {'service_id': None, 'favorite': None,
                                        'include_blocked': False, 'newest_first': True,
                                        'limit': FIRST_PAGE}})
                snapshot = next_frame(lambda f: f.get('type') == 'snapshot',
                                      context='snapshot')
                self.assertEqual(snapshot.get('request_id'), subscribe_request)
                generation = int(snapshot.get('generation', 0))
                self.assertGreaterEqual(generation, 1)
                items = snapshot.get('items') or []
                self.assertEqual(len(items), FIRST_PAGE,
                                 f'snapshot must hold the first page, got {len(items)}')
                snapshot_ids = {item['flow_id'] for item in items}
                self.assertNotIn(old_id, snapshot_ids,
                                 'the old archive flow must be outside the first page')
                self.assertIn(decoy_id, snapshot_ids)
                self.assertIn(active_id, snapshot_ids)
                self.assertEqual({item['service_id'] for item in items} & {bulk_id, archive_id},
                                 {bulk_id, archive_id},
                                 'the first page must contain both services')

                cursor = snapshot.get('next_cursor')
                self.assertIsNotNone(cursor, 'snapshot of a fuller history needs next_cursor')
                page_request = str(uuid.uuid4())
                ws.send_json({'type': 'page', 'request_id': page_request,
                              'after': cursor, 'limit': FIRST_PAGE})
                flows_page = next_frame(lambda f: f.get('type') == 'flows_page',
                                        context='flows_page')
                self.assertEqual(flows_page.get('request_id'), page_request)
                self.assertGreaterEqual(int(flows_page.get('generation', 0)), generation)
                page_ids = {item['flow_id'] for item in flows_page.get('items') or []}
                self.assertIn(old_id, page_ids,
                              'flows_page did not reach the older archive flow')

                # Live flow update over the same socket: a stream opened after
                # subscribe must be pushed as a `flow` frame, not polled.
                live_payload = b'live update flow\n'
                live_sock, live_port, echoed = exchange(
                    live_payload, listen_port=archive_port)
                self.assertEqual(echoed, live_payload)
                live_frame = next_frame(
                    lambda f: f.get('type') == 'flow'
                    and (f.get('event') or {}).get('service_id') == archive_id
                    and (f.get('event') or {}).get('kind') == 'flow_opened',
                    context='live flow_opened update')
                live_flow = wait_for(lambda: flow_by_port(archive_id, live_port))
                assert live_flow is not None, 'Core did not index the live flow'
                self.assertEqual((live_frame.get('event') or {})['flow_id'],
                                 live_flow['flow_id'],
                                 'live flow frame does not match the opened flow')
                self.assertGreaterEqual(int(live_frame.get('generation', 0)), generation)

                # ---- Selected-flow live bytes over WS ------------------------
                select_request = str(uuid.uuid4())
                ws.send_json({'type': 'select', 'request_id': select_request,
                              'flow_id': active_id, 'active_bytes': True})
                streams = ByteStreams()
                first_active = active_frames_until(
                    lambda f: (streams.apply(f), True)[1] and
                    streams.assembled(1) == initial and streams.assembled(2) == initial,
                    context='initial active_bytes snapshot')
                self.assertEqual(first_active.get('request_id'), select_request)
                self.assertGreaterEqual(int(first_active.get('generation', 0)), generation)
                self.assertTrue(first_active['snapshot'].get('active') is True,
                                'the selected flow is still open, active must be true')

                # The page path must not have used the legacy HTTP history route.
                rows = wait_for(lambda: audit_rows(active_id))
                self.assertIsNotNone(rows, 'WS select wrote no audit row')
                actions = [row[0] for row in rows]
                self.assertNotIn('traffic.history.read', actions,
                                 'WS select fell back to the legacy HTTP history route')
                subscribes = [row for row in rows if row[0] == 'traffic.history.subscribe']
                self.assertEqual(len(subscribes), 1,
                                 f'expected one history subscribe audit row, got {rows}')
                self.assertEqual(subscribes[0][1], select_request,
                                 'history subscribe audit request_id must echo the select frame')
                self.assertEqual(int(subscribes[0][2]), 1)

                # Incremental append: only the new chunk sequence, no duplicate.
                extra = b'-second-chunk'
                active_client.sendall(extra)
                self.assertEqual(read_exact(active_client, len(extra)), extra)
                full = initial + extra
                seen = []
                saw_live_message = False
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    frame = ws.recv_json(timeout=2.0)
                    if frame is None:
                        continue
                    self.assertNotEqual(frame.get('type'), 'error', frame)
                    if frame.get('type') == 'message':
                        # Only the selected flow's live messages are forwarded.
                        self.assertEqual(frame.get('flow_id'), active_id, frame)
                        saw_live_message = True
                        continue
                    if frame.get('type') != 'active_bytes':
                        continue
                    streams.apply(frame)
                    assembled = streams.assembled(1)
                    seen.append(assembled)
                    if assembled == full and streams.assembled(2) == full:
                        break
                else:
                    self.fail('incremental active_bytes frames did not reach the full stream')
                self.assertTrue(saw_live_message,
                                'the selected flow produced no live `message` frame over WS')
                self.assertTrue(seen, 'no active_bytes frame carried the new chunk')
                for assembled in seen:
                    # Before the 64 KiB bound, every assembled view must be a
                    # prefix of what was forwarded: a duplicated or reordered
                    # chunk would break this, regardless of reset/append mode.
                    self.assertTrue(full.startswith(assembled),
                                    'incremental assembly duplicated or reordered bytes')
                self.assertEqual(streams.assembled(1), full,
                                 'incremental active bytes duplicated or lost data')
                self.assertEqual(streams.assembled(2), full,
                                 'server-direction active bytes duplicated or lost data')

                # ---- Pause/resume around the blocking burst ------------------
                # The client contract (docs/traffic-ws-frame-contract.md) gives a
                # non-reading client `pause`, and the real frontend uses it when
                # the tab is hidden. This fixture is single-threaded, so the
                # 80 KiB echo round trip and the auth refresh below block the only
                # reader. Pausing first stops live frames and the active_bytes
                # poll, so the documented 4 MiB / 64-frame queue is never asked to
                # buffer a burst the client cannot drain (a genuine 1013
                # `slow consumer`, not a reason to raise the bounds).
                ws.send_json({'type': 'pause'})
                # Inbound frames are handled in order on one socket, so a reply to
                # a frame sent *after* `pause` proves the pause took effect.
                pause_barrier = str(uuid.uuid4())
                ws.send_json({'type': 'messages', 'request_id': pause_barrier,
                              'flow_id': old_id, 'after': None, 'limit': 1})
                next_frame(lambda f: f.get('type') == 'messages_page' and
                           f.get('request_id') == pause_barrier,
                           context='messages_page proving pause was applied')
                # Stop the selected-flow poll too: `select` with a null flow_id
                # clears the selection, so no active_bytes frame can be produced
                # while the client is blocked.
                ws.send_json({'type': 'select', 'request_id': str(uuid.uuid4()),
                              'flow_id': None})

                # Bounded blocking work while paused: 80 KiB echo + refresh.
                burst = bytes(range(256)) * 320  # 81920 bytes, non-repeating tail
                active_client.sendall(burst)
                self.assertEqual(read_exact(active_client, len(burst)), burst)
                full += burst
                expected_tail = full[-MAX_DIRECTION_BYTES:]
                session = refresh_session(session)
                alice = session[0]

                # Resume re-snapshots under a new generation; refresh the in-band
                # token right away so the resumed generation is not cut off by the
                # 60 s access-token clock.
                resume_request = str(uuid.uuid4())
                ws.send_json({'type': 'resume', 'request_id': resume_request})
                resumed = next_frame(
                    lambda f: f.get('type') == 'snapshot' and
                    f.get('request_id') == resume_request,
                    context='snapshot after resume')
                resumed_generation = int(resumed.get('generation', 0))
                self.assertGreater(resumed_generation, generation,
                                   'resume must re-snapshot under a newer generation')
                resumed_items = resumed.get('items') or []
                self.assertEqual(len(resumed_items), FIRST_PAGE,
                                 'the resumed snapshot must be the full first page')
                resumed_ids = {item['flow_id'] for item in resumed_items}
                self.assertIn(active_id, resumed_ids)
                self.assertIn(decoy_id, resumed_ids)
                self.assertNotIn(old_id, resumed_ids,
                                 'the resumed snapshot must keep the same first page')
                ws.send_json({'type': 'auth', 'request_id': str(uuid.uuid4()),
                              'token': session[0]})
                next_frame(lambda f: f.get('type') == 'ready',
                           context='ready after the refreshed token')

                # Re-select the real flow on the resumed generation: `select`
                # resets the history cursor, so the retained tail is read from
                # scratch instead of continuing the pre-pause cursor.
                reselect_request = str(uuid.uuid4())
                ws.send_json({'type': 'select', 'request_id': reselect_request,
                              'flow_id': active_id, 'active_bytes': True})
                overflow = active_frames_until(
                    lambda f: int(f.get('generation', 0)) >= resumed_generation
                    and (streams.apply(f), True)[1] and
                    int((f['snapshot'].get('retained_client_to_server_bytes')
                         if f['snapshot'].get('retained_client_to_server_bytes') is not None
                         else sum(len(bytes(s['data']))
                                  for s in f['snapshot'].get('segments') or []
                                  if int(s['direction']) == 1))) == MAX_DIRECTION_BYTES,
                    context='bounded 64 KiB tail reset after resume')
                self.assertEqual(overflow.get('request_id'), reselect_request,
                                 'the bounded tail must come from the post-resume reselect')
                self.assertGreaterEqual(int(overflow.get('generation', 0)),
                                        resumed_generation)
                self.assertTrue(overflow['snapshot'].get('active') is True,
                                'the re-selected flow is still open, active must be true')
                # The reselect is a new audited sensitive read; the audit row is
                # committed before any bytes are polled, so it is already visible
                # when the frame arrives (no blocking wait on an unread socket).
                reselect_rows = [row for row in audit_rows(active_id)
                                 if row[0] == 'traffic.history.subscribe'
                                 and row[1] == reselect_request]
                self.assertEqual(len(reselect_rows), 1,
                                 'the post-resume reselect must write exactly one '
                                 f'history subscribe audit row, got {reselect_rows}')
                self.assertEqual(int(reselect_rows[0][2]), 1)
                self.assertTrue(overflow['snapshot'].get('reset'),
                                'head eviction must force reset=true')
                self.assertTrue(overflow['snapshot'].get('truncated_client_to_server'))
                self.assertGreater(int(overflow['snapshot'].get('client_head_dropped', 0)), 0,
                                   'head drops must be counted, not silently spliced')
                self.assertEqual(streams.assembled(1), expected_tail,
                                 'retained active bytes are not the exact 64 KiB tail')
                self.assertLessEqual(len(streams.assembled(2)), MAX_DIRECTION_BYTES)

                # ---- Global retained search over the same socket -------------
                search_request = str(uuid.uuid4())
                ws.send_json({'type': 'search', 'request_id': search_request,
                              'q': MARKER, 'scope': 'both', 'service_id': None,
                              'after': None, 'limit': 50})
                search = next_frame(lambda f: f.get('type') == 'search_page',
                                    context='search_page')
                self.assertEqual(search.get('request_id'), search_request)
                self.assertGreaterEqual(int(search.get('generation', 0)), generation)
                coverage = search.get('coverage') or {}
                self.assertTrue(coverage.get('decoded_only'))
                self.assertFalse(coverage.get('raw_history_searched'))
                hits = search.get('items') or []
                self.assertTrue(hits, 'search found no hit for the retained old flow')
                self.assertEqual({item['flow_id'] for item in hits}, {old_id},
                                 'literal search leaked the needleXone decoy or missed the old flow')
                self.assertNotIn(decoy_id, {item['flow_id'] for item in hits})
                self.assertTrue(all(item.get('kind') == 'message' for item in hits), hits)
                self.assertTrue(all(item.get('service_id') == archive_id for item in hits))
                matched = 0
                flag_matched = 0
                for item in hits:
                    snippet = item.get('snippet')
                    self.assertIsInstance(snippet, str)
                    self.assertIn(MARKER, snippet)
                    for start, end in spans_of(item, 'spans'):
                        self.assertLessEqual(end, len(snippet.encode('utf-16-le')) // 2)
                        if utf16_slice(snippet, start, end) == MARKER:
                            matched += 1
                    for start, end in spans_of(item, 'flag_spans'):
                        if utf16_slice(snippet, start, end) == FLAG:
                            flag_matched += 1
                self.assertGreater(matched, 0,
                                   'search match spans do not slice to the literal query '
                                   'in UTF-16 code units')
                self.assertGreater(flag_matched, 0,
                                   'search flag_spans do not slice to the configured flag '
                                   'in UTF-16 code units (emoji before the flag)')

                # Backslash is literal, not a regex escape.
                backslash_request = str(uuid.uuid4())
                ws.send_json({'type': 'search', 'request_id': backslash_request,
                              'q': BACKSLASH_QUERY, 'scope': 'both', 'service_id': None,
                              'after': None, 'limit': 50})
                backslash = next_frame(
                    lambda f: f.get('type') == 'search_page' and
                    f.get('request_id') == backslash_request,
                    context='backslash search_page')
                self.assertEqual({item['flow_id'] for item in backslash.get('items') or []},
                                 {old_id},
                                 'a query with backslashes was not matched literally')

                # ---- Selected-flow messages + flag spans over WS -------------
                messages_request = str(uuid.uuid4())
                ws.send_json({'type': 'messages', 'request_id': messages_request,
                              'flow_id': old_id, 'after': None, 'limit': 100})
                messages = next_frame(lambda f: f.get('type') == 'messages_page',
                                      context='messages_page')
                self.assertEqual(messages.get('request_id'), messages_request)
                self.assertEqual(messages.get('flow_id'), old_id)
                message_items = messages.get('items') or []
                self.assertTrue(message_items, 'messages_page returned no old-flow items')
                directions = {int(item['direction']) for item in message_items}
                self.assertEqual(directions, {1, 2},
                                 'both directions of the old flow must be returned')
                server_flag_spans = 0
                for item in message_items:
                    text, source = message_text(item)
                    self.assertIsNotNone(
                        text, 'messages_page item has no display text for flag_spans')
                    self.assertIn(FLAG, text)
                    item_spans = spans_of(item, 'flag_spans')
                    self.assertTrue(item_spans,
                                    f'messages_page item ({source}) has no flag_spans')
                    for start, end in item_spans:
                        if utf16_slice(text, start, end) == FLAG:
                            if int(item['direction']) == 2:
                                server_flag_spans += 1
                self.assertGreater(server_flag_spans, 0,
                                   'the server-answer message has no correct UTF-16 flag span')

                # ---- Legacy HTTP 409 is expected, and audited coherently -----
                # The HTTP request below plus its audit wait block this
                # single-threaded reader while the selected-flow poll keeps
                # pushing at 1 Hz. Pause first (the same client contract used
                # around the burst above) so the bounded 4 MiB / 64-frame queue
                # is never asked to buffer frames the reader cannot drain; the
                # queue bounds themselves stay unchanged.
                ws.send_json({'type': 'pause'})
                audit_barrier = str(uuid.uuid4())
                ws.send_json({'type': 'messages', 'request_id': audit_barrier,
                              'flow_id': active_id, 'after': None, 'limit': 1})
                next_frame(lambda f: f.get('type') == 'messages_page' and
                           f.get('request_id') == audit_barrier,
                           context='messages_page proving pause before the legacy probe')
                # Clearing the selection also stops the active_bytes poll, so no
                # frame can be produced while the audit wait blocks the reader.
                ws.send_json({'type': 'select', 'request_id': str(uuid.uuid4()),
                              'flow_id': None})

                legacy_key = str(uuid.uuid4())
                status, body, raw = request('GET', f'/traffic/flows/{active_id}/history',
                                            token=alice, key=legacy_key)
                self.assertEqual(status, 409,
                                 f'legacy HTTP history must stay 409 for an active flow, '
                                 f'got {status} {raw[:200]!r}')
                legacy_rows = wait_for(lambda: (
                    lambda rows: rows if {row[0] for row in rows} >=
                    {'traffic.history.read', 'traffic.history.result'} else None
                )(audit_rows(active_id)), timeout=10)
                self.assertIsNotNone(legacy_rows, 'legacy history read was not audited')
                read_rows = [row for row in legacy_rows if row[0] == 'traffic.history.read']
                result_rows = [row for row in legacy_rows if row[0] == 'traffic.history.result']
                self.assertEqual(len(read_rows), 1, legacy_rows)
                self.assertEqual(len(result_rows), 1, legacy_rows)
                self.assertTrue(read_rows[0][1])
                self.assertEqual(read_rows[0][1], result_rows[0][1],
                                 'history intent and result audit rows must share one request id '
                                 '(audit id correlation)')
                self.assertEqual(int(result_rows[0][2]), 0,
                                 'the 409 result must be audited as unsuccessful')

                # Resume re-snapshots under a newer generation; the audit wait
                # above cleared the selection, so re-select the still-open flow
                # before the close phase. The select ack is what proves the
                # selection is live again (not assumed from the pre-pause one).
                audit_resume_request = str(uuid.uuid4())
                ws.send_json({'type': 'resume', 'request_id': audit_resume_request})
                audit_resumed = next_frame(
                    lambda f: f.get('type') == 'snapshot' and
                    f.get('request_id') == audit_resume_request,
                    context='snapshot after the audit-wait resume')
                audit_generation = int(audit_resumed.get('generation', 0))
                self.assertGreater(audit_generation, resumed_generation,
                                   'the resume around the legacy audit must use a newer '
                                   'generation than the burst resume')
                self.assertIn(active_id,
                              {item['flow_id'] for item in audit_resumed.get('items') or []},
                              'the resumed snapshot lost the still-open flow')

                post_audit_select = str(uuid.uuid4())
                ws.send_json({'type': 'select', 'request_id': post_audit_select,
                              'flow_id': active_id, 'active_bytes': True})
                post_audit_tail = active_frames_until(
                    lambda f: f.get('request_id') == post_audit_select
                    and int(f.get('generation', 0)) >= audit_generation
                    and (streams.apply(f), True)[1],
                    context='post-audit reselect active_bytes')
                self.assertEqual(post_audit_tail.get('request_id'), post_audit_select,
                                 'the post-audit reselect must be the one polling bytes')
                self.assertGreaterEqual(int(post_audit_tail.get('generation', 0)),
                                        audit_generation)
                self.assertTrue(post_audit_tail['snapshot'].get('active') is True,
                                'the re-selected flow is still open after the audit wait')
                self.assertEqual(streams.assembled(1), expected_tail,
                                 'the post-audit reselect must restore the exact retained tail')
                # One audit row per select request, scoped by request_id: this
                # select is new, so it adds exactly one row of its own.
                post_audit_rows = [row for row in audit_rows(active_id)
                                   if row[0] == 'traffic.history.subscribe'
                                   and row[1] == post_audit_select]
                self.assertEqual(len(post_audit_rows), 1,
                                 'each select request must write exactly one history '
                                 f'subscribe audit row, got {post_audit_rows}')
                self.assertEqual(int(post_audit_rows[0][2]), 1)

                # ---- Close: final persisted history over WS, no duplicates ---
                active_client.shutdown(socket.SHUT_WR)
                try:
                    while active_client.recv(4096):
                        pass
                except (ConnectionResetError, socket.timeout):
                    pass
                active_client.close()
                active_client = None

                final = None
                closed_frames = []
                quiet_since = 0.0
                deadline = time.monotonic() + 25
                while time.monotonic() < deadline:
                    frame = ws.recv_json(timeout=2.0)
                    if frame is None:
                        # Stop once the persisted frame is in and nothing new
                        # has arrived for a few seconds (the poll is 1 Hz).
                        if final is not None and time.monotonic() - quiet_since > 5:
                            break
                        continue
                    if frame.get('type') == 'error':
                        self.fail(f'WS error while closing the flow: {frame}')
                    if frame.get('type') != 'active_bytes':
                        continue
                    snapshot = frame.get('snapshot') or {}
                    if snapshot.get('active') is True:
                        continue
                    closed_frames.append(frame)
                    streams.apply(frame)
                    final = frame
                    quiet_since = time.monotonic()
                    # For a closed flow the Core DTO reports the total forwarded
                    # bytes here (the segments themselves are the retained tail).
                    totals = snapshot.get('retained_client_to_server_bytes')
                    if totals is not None and int(totals) == len(full):
                        break
                self.assertIsNotNone(final, 'closing the flow produced no active_bytes frame')
                self.assertFalse(final['snapshot'].get('active'))
                for frame in closed_frames:
                    for direction in (1, 2):
                        # Chunk sequences are per-direction counters, so the
                        # same value in both directions is normal; a repeat
                        # within one direction is the duplicate-assembly bug.
                        seqs = [int(s['sequence'])
                                for s in frame['snapshot'].get('segments') or []
                                if int(s['direction']) == direction]
                        self.assertEqual(len(seqs), len(set(seqs)),
                                         f'final history repeated a chunk sequence '
                                         f'in direction {direction}')
                    assembled = b''.join(
                        bytes(s['data']) for s in sorted(
                            (s for s in frame['snapshot'].get('segments') or []
                             if int(s['direction']) == 1),
                            key=lambda s: int(s['sequence'])))
                    self.assertTrue(full.endswith(assembled),
                                    'a final-history frame assembled bytes that are not a '
                                    'suffix of the forwarded stream (duplicate assembly)')
                self.assertEqual(streams.assembled(1), expected_tail,
                                 'final WS history is not the exact persisted 64 KiB tail')
                snapshot = final['snapshot']
                self.assertEqual(int(snapshot.get('retained_client_to_server_bytes', -1)),
                                 len(full))
                self.assertTrue(snapshot.get('truncated_client_to_server'))

                # Legacy HTTP route now serves the same persisted history (200).
                status, saved, raw = request(
                    'GET', f'/traffic/flows/{active_id}/history', token=alice)
                self.assertEqual(status, 200, raw[:400])
                self.assertEqual(saved['bytes_client_to_server'], len(full))
                http_tail = b''.join(bytes(segment['data'])
                                     for segment in saved['segments']
                                     if segment['direction'] == 1)
                self.assertEqual(http_tail, expected_tail,
                                 'legacy HTTP history and WS final history disagree')

                # ---- Re-auth and reconnect re-snapshot -----------------------
                session = refresh_session(session)
                ws.send_json({'type': 'auth', 'request_id': str(uuid.uuid4()),
                              'token': session[0]})
                ws.send_json({'type': 'subscribe', 'request_id': str(uuid.uuid4()),
                              'query': {'service_id': None, 'favorite': None,
                                        'include_blocked': False, 'newest_first': True,
                                        'limit': 10}})
                resnapshot = next_frame(lambda f: f.get('type') == 'snapshot',
                                        context='snapshot after in-band re-auth')
                self.assertEqual(len(resnapshot.get('items') or []), 10)

                ws.close()
                ws = None
                ws = WebSocket(http_port)
                session = refresh_session(session)
                ws.send_json({'type': 'auth', 'request_id': str(uuid.uuid4()),
                              'token': session[0]})
                next_frame(lambda f: f.get('type') == 'ready', context='ready on reconnect')
                ws.send_json({'type': 'subscribe', 'request_id': str(uuid.uuid4()),
                              'query': {'service_id': None, 'favorite': None,
                                        'include_blocked': False, 'newest_first': True,
                                        'limit': 10}})
                reconnect_snapshot = next_frame(
                    lambda f: f.get('type') == 'snapshot',
                    context='snapshot after reconnect')
                self.assertEqual(len(reconnect_snapshot.get('items') or []), 10)
            finally:
                if ws is not None:
                    ws.close()
                if active_client is not None:
                    try:
                        active_client.close()
                    except OSError:
                        pass
                for process in processes:
                    if process.poll() is None:
                        process.terminate()
                    process.wait(timeout=10)
                stop.set()
                upstream.close()
                log.close()


if __name__ == '__main__':
    unittest.main()
