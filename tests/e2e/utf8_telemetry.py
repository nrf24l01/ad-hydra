"""Regression: built-in UTF-8 stream telemetry under a multi-MiB burst.

Real two-process harness (`hydra-proxy` + `hydra-core`) plus a loopback echo
upstream, driven through the public HTTP API only. The service uses
``protocol_id: "utf8"`` — the built-in decoder alias at
``apps/hydra-proxy/src/inspection/protocols.rs`` — with **no** protocol
profile, activation or WASM anywhere in the test (asserted below).

Covered:
  * a 3 MiB request and its 3 MiB echo are forwarded byte-for-byte intact in
    both directions through the utf8 service;
  * Core indexes the closed flow with byte totals equal to the burst size;
  * per-flow history stays bounded: each direction stores at most
    ``MAX_DIRECTION_BYTES`` (64 KiB, ``apps/hydra-proxy/src/history/mod.rs``)
    and reports the documented truncation flags; the retained bytes are the
    exact tail of the forwarded stream, not arbitrary or reordered data;
  * decoded ``utf8`` messages are still produced, each text chunk <= 4096
    chars (``crates/hydra-protocol-runtime/src/utf8.rs`` ``TEXT_CHUNK``), the
    text survived JSON escaping, and both directions are an in-order
    subsequence of the expected text — i.e. under load chunks may be *absent*
    (telemetry is explicitly lossy, ``docs/PERFORMANCE.md``) but never
    reordered, duplicated, escaped or corrupted;
  * ``telemetry_dropped`` is monotonic and settles under the bounded workload;
    subscriber-lag loss is attributed through the public global SSE stream
    (``GET /api/v1/traffic/events/stream``), never by reading the compressed
    SQLite view (which needs Rust-only ``hydra_text``). Lag markers are global:
    their ``flow_id``/``service_id`` are empty, so the collector must not
    filter by flow.
  * ``indexing_dropped`` is settled and unchanged across the burst. The
    workload is isolated (one utf8 service, one flow) and every decoded chunk
    is valid UTF-8 far below Core's 64 KiB JSON-row bound, so Core must accept
    every batch; a nonzero delta means one poisoned event made Core reject a
    whole batch during conversion/insert (all-or-nothing ``convert()``), which
    is a Proxy-payload/Core-ingest fault, not an SSE observer gap.

Deliberately not asserted: that ``telemetry_dropped`` stays zero. Loss is the
documented behaviour of an overloaded bounded pipeline; the invariant is that
it is reported and attributed, not that it never happens. If the SSE consumer
itself lags, attribution proof is incomplete and the test fails explicitly
instead of treating missing evidence as success.

Failure text distinguishes the two evidence classes: an SSE observer that
overflowed (``event: loss``) or reconnected can hide markers, so a marker/counter
mismatch under those conditions is reported as an observer evidence gap rather
than as a Proxy telemetry error; a mismatch with a connected, lag-free observer
is reported as a telemetry accounting error. ``indexing_dropped`` needs no SSE
evidence at all, so it is never blamed on the observer.

Run after ``cargo build --workspace``:
    python3 tests/e2e/utf8_telemetry.py
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

BURST_BYTES = 3 * 1024 * 1024
MAX_DIRECTION_BYTES = 64 * 1024
TEXT_CHUNK = 4096
ORIGIN = 'https://hydra.test'


def burst_payload(size=BURST_BYTES):
    """Deterministic valid UTF-8, mixing ASCII with 2/3/4-byte characters so
    the decoder has to split on character boundaries rather than on bytes."""
    atoms = ['plain ascii line\n', 'caf\u00e9 \u00fcber\n', '\u4e16\u754c\u306e\u30c6\u30b9\u30c8\n',
             '\U0001f680 launch \U0001f9ea\n', 'GET /index.html HTTP/1.1\r\n\r\n']
    out = []
    total = 0
    index = 0
    while total < size:
        atom = atoms[index % len(atoms)]
        encoded = atom.encode('utf-8')
        out.append(encoded)
        total += len(encoded)
        index += 1
    return b''.join(out)[:size]


def read_exact(sock, count):
    data = b''
    while len(data) < count:
        chunk = sock.recv(min(65536, count - len(data)))
        if not chunk:
            raise AssertionError(f'connection closed after {len(data)}/{count} bytes')
        data += chunk
    return data


def field_text(item):
    fields = item.get('fields') or {}
    for key in ('utf8.text', 'text', 'payload'):
        value = fields.get(key)
        if isinstance(value, dict) and isinstance(value.get('value'), str):
            return value['value']
        if isinstance(value, str):
            return value
    return None


class Utf8Telemetry(unittest.TestCase):
    def test_builtin_utf8_burst_bytes_history_and_reported_loss(self):
        payload = burst_payload()
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
        with tempfile.TemporaryDirectory(prefix='hydra-utf8-') as directory:
            tmp = pathlib.Path(directory)
            proxy_port, http_port = port(), port()
            sock = str(tmp / 'proxy.sock')
            config = {'socket': sock, 'state_file': str(tmp / 'saved.json'),
                      'snapshot': {'revision': 0, 'services': []}}
            (tmp / 'proxy.json').write_text(json.dumps(config))
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
                return status, json.loads(raw) if raw else None

            def good(method, path, body=None, token=None, key=None):
                status, reply = request(method, path, body, token, key)
                self.assertIn(status, [200, 201, 202, 204], (status, reply))
                return reply

            def wait_for(fn, timeout=15.0):
                deadline = time.monotonic() + timeout
                while time.monotonic() < deadline:
                    value = fn()
                    if value:
                        return value
                    time.sleep(.05)
                return None

            def proxy_status(token):
                return good('GET', '/system/proxy', token=token)

            # Public global event stream: this replaces reading the compressed
            # ``traffic_events`` SQLite view, which needs Core's Rust-registered
            # ``hydra_text`` function and cannot be queried from plain sqlite3.
            sse = {'loss': [], 'lag': [], 'errors': [], 'connections': 0,
                   'reconnects': 0}
            sse_lock = threading.Lock()
            sse_stop = threading.Event()
            sse_ready = threading.Event()
            sse_conn = {'conn': None}

            def sse_login():
                status, reply = request(
                    'POST', '/auth/login',
                    {'username': 'alice', 'password': 'test-password-1234'})
                if status != 200:
                    raise RuntimeError(f'event stream login failed: {status}')
                return reply['access_token']

            def sse_worker():
                while not sse_stop.is_set():
                    conn = None
                    try:
                        token = sse_login()
                        conn = http.client.HTTPConnection(
                            '127.0.0.1', http_port, timeout=10)
                        with sse_lock:
                            sse_conn['conn'] = conn
                        conn.request('GET', '/api/v1/traffic/events/stream',
                                     headers={'Origin': ORIGIN,
                                              'Authorization': 'Bearer ' + token})
                        response = conn.getresponse()
                        if response.status != 200:
                            raise RuntimeError(
                                f'event stream status {response.status}')
                        # Blocking reads: the main thread unblocks us by
                        # closing the socket during cleanup; axum keep-alives
                        # also wake us every 10 s.
                        if conn.sock is not None:
                            conn.sock.settimeout(None)
                        with sse_lock:
                            sse['connections'] += 1
                            # Earlier attempts before the first successful
                            # subscription are not evidence gaps.
                            sse['errors'].clear()
                        sse_ready.set()
                        name = None
                        data = []
                        while not sse_stop.is_set():
                            try:
                                line = response.readline()
                            except AttributeError as exc:
                                # ``HTTPConnection.close()`` (the teardown
                                # unblock path) also closes the response and
                                # clears ``response.fp``, so a worker blocked
                                # in readline()->peek() can observe None.
                                # Only the deliberate stop may be absorbed;
                                # any other AttributeError is a real collector
                                # failure and must be reported below.
                                if sse_stop.is_set():
                                    break
                                raise RuntimeError(
                                    f'event stream read failed: {exc!r}') from exc
                            if not line:
                                break
                            text = line.decode('utf-8', 'replace').rstrip('\r\n')
                            if not text:
                                if name == 'traffic' and data:
                                    try:
                                        item = json.loads('\n'.join(data))
                                    except ValueError:
                                        item = None
                                    if (item is not None
                                            and item.get('kind') == 'event_loss'
                                            and item.get('reason') == 'subscriber_lag'):
                                        # Global marker: flow_id/service_id are
                                        # empty by construction; count it
                                        # regardless of scope.
                                        with sse_lock:
                                            sse['loss'].append(
                                                int(item.get('count', 0)))
                                elif name == 'loss' and data:
                                    with sse_lock:
                                        sse['lag'].append(data[0])
                                name, data = None, []
                            elif text.startswith(':'):
                                continue
                            elif text.startswith('event:'):
                                name = text[6:].strip()
                            elif text.startswith('data:'):
                                data.append(text[5:].lstrip())
                    except (OSError, http.client.HTTPException, RuntimeError, ValueError) as exc:
                        if not sse_stop.is_set():
                            with sse_lock:
                                sse['errors'].append(repr(exc))
                    finally:
                        if conn is not None:
                            try:
                                conn.close()
                            except OSError:
                                pass
                        with sse_lock:
                            sse_conn['conn'] = None
                    if sse_stop.is_set():
                        return
                    # Access JWTs live 60 s; the server closes the SSE stream at
                    # expiry. Re-authenticate and resubscribe, and account the
                    # reconnect explicitly (it is a possible evidence gap).
                    with sse_lock:
                        sse['reconnects'] += 1
                    time.sleep(.05)

            sse_thread = threading.Thread(target=sse_worker, daemon=True)

            def sse_snapshot():
                with sse_lock:
                    return (sum(sse['loss']), list(sse['lag']),
                            sse['reconnects'], list(sse['errors']))

            def wait_marker_sum(target, quiet=1.0, timeout=12.0):
                """Wait until the SSE loss sum reaches ``target`` and stays
                there for ``quiet`` seconds, so pending markers are drained
                before the delta is compared."""
                deadline = time.monotonic() + timeout
                last = None
                quiet_since = time.monotonic()
                while True:
                    total = sse_snapshot()[0]
                    if total != last:
                        last = total
                        quiet_since = time.monotonic()
                    if total >= target and time.monotonic() - quiet_since >= quiet:
                        return sse_snapshot()
                    if time.monotonic() >= deadline:
                        return sse_snapshot()
                    time.sleep(.1)

            def settled_proxy_status(timeout=20.0, samples=6, interval=0.5):
                """Bounded workload: wait for ``telemetry_dropped`` *and*
                ``indexing_dropped`` to stop moving before deriving deltas.
                Core refreshes the proxy view once per second, so require >2 s
                of identical readings to span several status ticks instead of
                trusting a stale snapshot."""
                deadline = time.monotonic() + timeout
                last = None
                stable = 0
                while time.monotonic() < deadline:
                    view = proxy_status(alice)
                    value = (view['telemetry_dropped'], view['indexing_dropped'])
                    if value == last:
                        stable += 1
                        if stable >= samples:
                            return view
                    else:
                        last = value
                        stable = 0
                    time.sleep(interval)
                return None

            try:
                subprocess.run(args + ['create', 'user', 'alice', 'test-password-1234'],
                               check=True, stdout=log, stderr=log, timeout=10)
                processes.append(subprocess.Popen(
                    [str(BIN / 'hydra-proxy'), '--config', str(tmp / 'proxy.json')],
                    stdout=log, stderr=log))
                processes.append(subprocess.Popen(
                    args + ['serve', '--listen', f'127.0.0.1:{http_port}', '--origin', ORIGIN],
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

                # Core is listening and the user exists now, so the collector
                # can subscribe without recording pre-start connection errors.
                sse_thread.start()

                created = good('POST', '/services', {
                    'expected_revision': 0,
                    'transport': 'tcp',
                    'listen_address': f'127.0.0.1:{proxy_port}',
                    'upstream_address': f'127.0.0.1:{upstream.getsockname()[1]}',
                    'protocol_id': 'utf8',
                    'enabled': True,
                    'retain_history': True,
                }, alice)
                # CreatedServiceResponse is `{id, #[serde(flatten)] desired}`, so
                # the input fields live under the nested service snapshot.
                self.assertEqual(len(created['services']['services']), 1)
                self.assertEqual(created['services']['services'][0]['protocol_id'], 'utf8')
                self.assertTrue(wait_for(
                    lambda: proxy_status(alice)['applied_services_revision'] == 1),
                    'Proxy did not apply the utf8 service snapshot')

                # No custom decoder may be involved: the built-in alias needs
                # no profile and no activation.
                activations = good('GET', '/protocols', token=alice)['activations']
                self.assertEqual(activations, [], 'test must not rely on an activation')

                # Subscribe to the global event stream *before* the burst so
                # loss markers published during the burst are observed live.
                self.assertTrue(sse_ready.wait(10), 'event stream did not subscribe')
                sse_errors = sse_snapshot()[3]
                self.assertEqual(sse_errors, [], f'event stream collector failed: {sse_errors}')

                # Access JWTs are short-lived (60 s); take a fresh one for the
                # burst window so the polling below cannot start on a token
                # that expires mid-burst.
                alice = good('POST', '/auth/login',
                             {'username': 'alice', 'password': 'test-password-1234'})['access_token']

                before = proxy_status(alice)
                self.assertTrue(before['connected'])
                dropped_before = before['telemetry_dropped']
                indexing_before = before['indexing_dropped']
                received_before = before['events_received']

                client = socket.create_connection(('127.0.0.1', proxy_port), timeout=10)
                client.settimeout(30)
                client_port = client.getsockname()[1]
                for offset in range(0, len(payload), 65536):
                    client.sendall(payload[offset:offset + 65536])
                echoed = read_exact(client, len(payload))
                self.assertEqual(len(echoed), len(payload),
                                 'proxy truncated the server-to-client stream')
                self.assertEqual(echoed, payload,
                                 'utf8 service altered forwarded bytes')
                client.shutdown(socket.SHUT_WR)
                try:
                    while client.recv(65536):
                        pass
                except (ConnectionResetError, socket.timeout):
                    pass
                client.close()

                flow = wait_for(lambda: next(
                    (f for f in good(
                        'GET', '/traffic/flows?newest_first=true&limit=50', token=alice)['items']
                     if (f.get('client') or {}).get('port') == client_port), None))
                self.assertIsNotNone(flow, 'Core did not index the burst flow')
                flow_id = flow['flow_id']

                saved = wait_for(lambda: (
                    lambda reply: reply[1] if reply[0] == 200 else None)(
                        request('GET', f'/traffic/flows/{flow_id}/history', token=alice)))
                self.assertIsNotNone(saved, 'Proxy never exposed the closed flow history')

                self.assertEqual(saved['bytes_client_to_server'], len(payload))
                self.assertEqual(saved['bytes_server_to_client'], len(payload))
                for direction in (1, 2):
                    data = b''.join(bytes(segment['data'])
                                    for segment in saved['segments']
                                    if segment['direction'] == direction)
                    self.assertLessEqual(
                        len(data), MAX_DIRECTION_BYTES,
                        f'history direction {direction} exceeded the per-flow cap')
                    self.assertEqual(
                        len(data), MAX_DIRECTION_BYTES,
                        f'history direction {direction} must retain the full '
                        f'{MAX_DIRECTION_BYTES}-byte tail of a 3 MiB burst')
                    self.assertEqual(
                        data, payload[len(payload) - len(data):],
                        f'history direction {direction} is not the exact forwarded tail')
                self.assertTrue(saved['history_truncated'],
                                'history_truncated must flag the 3 MiB burst')
                self.assertTrue(saved['truncated_client_to_server'])
                self.assertTrue(saved['truncated_server_to_client'])

                # Decoded utf8 messages: bounded page walk, ordering only.
                items, cursor, pages = [], None, 0
                while pages < 25:
                    # Core bounds `limit` to 1..=100
                    # (apps/hydra-core/src/modules/traffic/repository.rs);
                    # 897 messages still walk in 9 pages under the 25-page cap.
                    path = f'/traffic/flows/{flow_id}/messages?limit=100'
                    if cursor is not None:
                        path += f'&after={cursor}'
                    page = good('GET', path, token=alice)
                    items.extend(page['items'])
                    cursor = page.get('next_cursor')
                    pages += 1
                    if not cursor:
                        break
                self.assertTrue(items, 'utf8 service produced no decoded messages')
                self.assertEqual({item['protocol_id'] for item in items}, {'utf8'},
                                 'built-in utf8 decoder must be the only producer')
                expected = payload.decode('utf-8')
                for direction in (1, 2):
                    chunks = sorted((item for item in items if item['direction'] == direction),
                                    key=lambda item: item['stream_sequence'])
                    self.assertTrue(chunks,
                                    f'no utf8 chunks indexed for direction {direction}')
                    position = 0
                    matched = 0
                    for item in chunks:
                        text = field_text(item)
                        self.assertIsNotNone(text, f'utf8 chunk has no text field: {item}')
                        self.assertLessEqual(len(text), TEXT_CHUNK,
                                             'utf8 text chunk exceeds TEXT_CHUNK')
                        if not text:
                            continue
                        # The fixture has no literal backslashes, so any
                        # backslash means the JSON text was not unescaped.
                        self.assertNotIn(
                            '\\', text,
                            f'utf8 chunk at sequence {item["stream_sequence"]} still '
                            'contains JSON escape backslashes')
                        found = expected.find(text, position)
                        self.assertNotEqual(
                            found, -1,
                            f'utf8 chunk at sequence {item["stream_sequence"]} in '
                            f'direction {direction} is not an in-order slice of the '
                            'forwarded text (corruption or reordering)')
                        position = found + len(text)
                        matched += 1
                    self.assertGreater(matched, 0)

                after = wait_for(lambda: (
                    lambda view: view if view['events_received'] > received_before else None)(
                        proxy_status(alice)), timeout=10)
                self.assertIsNotNone(after, 'telemetry pipeline stopped delivering events')
                self.assertGreaterEqual(
                    after['telemetry_dropped'], dropped_before,
                    'telemetry_dropped counter went backwards')

                settled = settled_proxy_status()
                self.assertIsNotNone(
                    settled, 'telemetry_dropped did not settle under a bounded workload')
                dropped_after = settled['telemetry_dropped']
                delta = dropped_after - dropped_before
                self.assertGreaterEqual(delta, 0)

                # Row-poisoning guard, independent of SSE evidence: this is an
                # isolated, valid-UTF-8 workload whose every chunk is far below
                # Core's 64 KiB JSON-row bound, so Core must convert and insert
                # every batch. A nonzero delta means one event made Core reject
                # a whole batch (all-or-nothing convert()), i.e. batching
                # poisoned healthy neighbours. That is a Proxy payload/Core
                # ingest fault, never an SSE observer overflow or reconnect.
                indexing_after = settled['indexing_dropped']
                self.assertEqual(
                    indexing_after, indexing_before,
                    'indexing_dropped moved '
                    f'{indexing_before} -> {indexing_after} during an isolated '
                    'valid-UTF-8 burst: Core rejected telemetry batches during '
                    'conversion/insert (one poisoned row discards every healthy '
                    'neighbour in the batch). This is a Proxy telemetry payload '
                    'or Core ingest error; it is not an SSE observer gap and no '
                    'SSE evidence is involved in this counter.')

                # Give Core time to ingest and publish every pending marker,
                # then require the global SSE loss sum to equal the counter
                # delta exactly. Lag markers are global (empty flow_id), so
                # this sums every subscriber_lag row, not a per-flow subset.
                total, sse_lag, reconnects, errors = wait_marker_sum(delta)
                self.assertEqual(errors, [], f'event stream collector failed: {errors}')
                if delta:
                    if total != delta:
                        if reconnects:
                            cause = ('the SSE observer reconnected '
                                     f'{reconnects} time(s), so markers published '
                                     'during a reconnect gap are unobservable: '
                                     'this is an observer evidence gap, not '
                                     'evidence of a Proxy telemetry error')
                        elif sse_lag:
                            cause = ('the SSE observer itself overflowed '
                                     f'(event: loss {sse_lag}), so markers '
                                     'published in that window are unobservable: '
                                     'this is an observer evidence gap, not '
                                     'evidence of a Proxy telemetry error')
                        else:
                            cause = ('the SSE observer stayed connected and '
                                     'lag-free, so no observer gap can explain '
                                     'this: the Proxy dropped counter and the '
                                     'Core-published subscriber_lag markers '
                                     'disagree, which is a telemetry accounting '
                                     'error')
                        self.fail(
                            f'telemetry_dropped advanced by {delta} but global SSE '
                            f'subscriber_lag markers sum to {total} '
                            f'(sse_lag={sse_lag}, reconnects={reconnects}): {cause}')
                else:
                    # ``delta == 0`` means the Proxy counted no lag at all, so
                    # there is no marker a reconnect gap could have hidden; the
                    # reconnect count is still reported. An observer overflow,
                    # by contrast, is a genuine hole in the absence proof.
                    if total:
                        self.fail(
                            f'global event stream reported {total} subscriber_lag '
                            'losses although telemetry_dropped did not move '
                            f'(sse_lag={sse_lag}, reconnects={reconnects}): the '
                            'observer saw markers the Proxy counter never '
                            'accounted for, a telemetry accounting error')
                    if sse_lag:
                        self.fail(
                            'cannot prove the absence of loss markers: the SSE '
                            f'observer itself overflowed (event: loss {sse_lag}); '
                            'this is an observer evidence gap, not a Proxy '
                            f'telemetry error (reconnects={reconnects})')
            finally:
                sse_stop.set()
                with sse_lock:
                    active = sse_conn['conn']
                if active is not None:
                    # Wake the worker's blocking readline() with a socket
                    # shutdown instead of conn.close(): close() also closes
                    # the response and clears ``response.fp`` mid-read, which
                    # races readline()/peek() into a spurious AttributeError.
                    try:
                        if active.sock is not None:
                            active.sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                if sse_thread.ident is not None:
                    sse_thread.join(timeout=5)
                    if sse_thread.is_alive():
                        # Bounded second stage: force the connection closed
                        # so no reader outlives the harness.
                        if active is not None:
                            try:
                                active.close()
                            except OSError:
                                pass
                        sse_thread.join(timeout=5)
                for process in processes:
                    if process.poll() is None:
                        process.terminate()
                    process.wait(timeout=10)
                stop.set()
                upstream.close()
                log.close()


if __name__ == '__main__':
    unittest.main()
