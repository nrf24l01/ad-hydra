"""Real-browser regression for the Streams workspace (CDP, no Playwright).

Boots the same two-process harness as ``streams_workspace.py`` -- real
``hydra-proxy`` + real ``hydra-core`` serving the *compiled* SPA over TLS, one
loopback UTF-8 echo upstream, two configured services -- logs in through the
real login form, and then judges the page by what its own socket actually
carries (``Network.webSocketFrameSent`` / ``Network.webSocketFrameReceived``)
instead of trusting a mocked DOM or a stubbed app response.

Proven here:
  * exactly one page socket (``wss://.../api/v1/traffic/events/socket``); the
    list, the selected flow and global search all ride it, and after the first
    ``snapshot`` the page issues no ``GET /api/v1/traffic/...`` flows/messages/
    search/history request at all;
  * the live indicator is judged on the real DOM rather than on a text prefix:
    the status element's *own* text node is exactly ``Live`` while the appended
    server counters live in a child ``<span>``, so the full status text reads
    ``Live · N open · N seen · N messages`` and never equals ``Live`` alone.
    The primary check is the shared ``browser.TRAFFIC_LIVE_EXPRESSION``
    predicate, not a local ``startsWith('Live')``;
  * selecting the still-open flow renders live capture bytes: a byte burst
    written to the upstream *after* selection appears in the read-only
    terminal, and the page received ``active_bytes`` for that flow;
  * a later server answer carrying the synthetic flag renders as a ``<mark>``
    whose text is exactly the flag, while an HTML probe in the same snippet
    stays literal text (no element created, no handler ran);
  * a global query finds a flow that is *outside* the first loaded page;
    opening it shows Core's real ``selected`` metadata -- the true client
    endpoint, TCP transport and Closed state -- not a fabricated row;
  * pause/resume is judged on a real tab-visibility transition: a foreground
    target is opened (``Target.createTarget`` + ``Target.activateTarget``) so
    the page's own ``document.visibilityState`` becomes ``hidden`` and it sends
    ``pause``; activating the original target makes it ``visible`` again and it
    sends ``resume``. No synthetic ``visibilitychange`` is dispatched and
    ``Page.setWebLifecycleState`` is not used. If this headless Chromium cannot
    expose real tab visibility, that subtest reports an explicit UNVERIFIED
    skip instead of claiming the behaviour works.

Prerequisites (checked, never guessed):
  * Chrome/Chromium on PATH -- resolved by ``browser.browser_binary()``;
  * ``target/<HYDRA_BUILD_PROFILE>/hydra-core`` built *after* ``frontend/dist``
    exists, because ``apps/hydra-core/build.rs`` embeds the SPA at compile time
    (a Core built before the frontend build serves a stale UI);
  * ``openssl`` on PATH for the throwaway TLS certificate.

Run like the other E2E harnesses (never against production values; every
fixture below is synthetic):

    cargo build --workspace          # after: cd frontend && npm run build
    python3 tests/e2e/browser_streams_workspace.py

Screenshots are written to ``$HYDRA_BROWSER_SCREENSHOT_DIR`` (default
``<repo>/scratch``) and contain only the synthetic fixtures above.
"""
import base64
import http.client
import json
import os
import pathlib
import re
import socket
import ssl
import struct
import subprocess
import tempfile
import threading
import time
import unittest

from relay import BIN, port
from browser import CDP, TRAFFIC_LIVE_EXPRESSION, browser_binary
from streams_workspace import read_exact

FIRST_PAGE = 100
FILLERS = 104

# Synthetic fixtures only. The pattern is a fixture; nothing here is a real
# flag, token or production value.
FLAG = 'FLAG{synthetic42}'
FLAG_PATTERN = r'FLAG\{[a-z0-9]+\}'
MARKER = 'needle.one'                       # literal dot; a regex would also match the decoy
DECOY = 'needleXone'
EMOJI = '\U0001f6a9'
LIVE_MARKER = 'live-bytes-synthetic-42'
HTML_PROBE = '<img src=x onerror="window.__xss=1"><b>literal</b>'
WS_PATH = '/api/v1/traffic/events/socket'
TRAFFIC_POLL = re.compile(r'/api/v1/traffic/(flows|messages|search|history)')

# The real DOM split behind ``TRAFFIC_LIVE_EXPRESSION``: the element's own
# first child is the primary state text node, the server counters are the
# single child ``<span>`` appended by ``TrafficPage.vue``.
LIVE_STATUS_FACTS = (
    "(()=>{const s=document.querySelector('#traffic-live-status');"
    "if(!s)return null;const primary=s.firstChild;"
    "const spans=[...s.querySelectorAll('span')];"
    "return {primary:primary?primary.textContent:null,"
    "primaryIsText:Boolean(primary)&&primary.nodeType===3,"
    "text:s.textContent,inner:s.innerText,"
    "spans:spans.map(e=>e.textContent)};})()")


def js(value):
    return json.dumps(value)


class RecordingCDP(CDP):
    """The repo's hand-rolled CDP client, plus ordered event capture.

    ``browser.CDP.call`` discards every message that is not the reply to the
    call in flight, which is all a navigation smoke test needs. A browser
    regression needs the opposite: the interleaved ``Network.*`` events are the
    evidence. This subclass keeps the base handshake/read/evaluate/click and
    only changes the read loop.
    """

    def __init__(self, address):
        super().__init__(address)
        self.events = []

    def call(self, method, params=None):
        self.sequence += 1
        payload = json.dumps({'id': self.sequence, 'method': method,
                              'params': params or {}}).encode()
        size = len(payload)
        header = (bytes([0x81, 0x80 | size]) if size < 126
                  else bytes([0x81, 0x80 | 126]) + struct.pack('!H', size))
        mask = os.urandom(4)
        self.socket.sendall(header + mask
                            + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))
        while True:
            start = self.read(2)
            length = start[1] & 127
            if length == 126:
                length = struct.unpack('!H', self.read(2))[0]
            elif length == 127:
                length = struct.unpack('!Q', self.read(8))[0]
            assert length < 4 * 1024 * 1024
            message = json.loads(self.read(length))
            if message.get('id') == self.sequence:
                assert 'error' not in message, message
                return message.get('result', {})
            if 'method' in message:
                self.events.append(message)

    # -- bounded event helpers -------------------------------------------
    def pump(self):
        # Browser-domain command: still answers while the renderer is frozen,
        # which is exactly when the lifecycle test needs to read events.
        self.call('Browser.getVersion')

    def since(self, index, method):
        return [event['params'] for event in self.events[index:]
                if event.get('method') == method]

    def frames(self, direction, index=0):
        """Sent/received WebSocket JSON frames; tokens are never returned."""
        out = []
        for params in self.since(index, 'Network.webSocketFrame' + direction):
            raw = (params.get('response') or {}).get('payloadData')
            try:
                frame = json.loads(raw)
            except (TypeError, ValueError):
                frame = {'type': '<non-json>'}
            out.append(frame if isinstance(frame, dict) else {'type': '<non-object>'})
        return out

    def types(self, direction, index=0):
        return [frame.get('type') for frame in self.frames(direction, index)]


def wait_until(cdp, predicate, timeout=20.0, context='condition'):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        cdp.pump()
        time.sleep(0.05)
    return False


def screenshot_dir():
    override = os.environ.get('HYDRA_BROWSER_SCREENSHOT_DIR')
    path = (pathlib.Path(override) if override
            else pathlib.Path(__file__).resolve().parents[2] / 'scratch')
    path.mkdir(parents=True, exist_ok=True)
    return path


class StreamsWorkspaceBrowser(unittest.TestCase):
    @unittest.skipUnless(browser_binary(), 'Chromium or Google Chrome is required')
    def test_streams_workspace_ui_over_page_socket(self):
        chrome = browser_binary()
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
        cdp = None
        log = None
        active_client = None
        with tempfile.TemporaryDirectory(prefix='hydra-browser-streams-') as directory:
            tmp = pathlib.Path(directory)
            bulk_port, archive_port, http_port, debug = (port(), port(), port(), port())
            origin = f'https://localhost:{http_port}'
            sock = str(tmp / 'proxy.sock')
            (tmp / 'proxy.json').write_text(json.dumps({
                'socket': sock, 'state_file': str(tmp / 'saved.json'),
                'snapshot': {'revision': 0, 'services': []},
                'flag_pattern': FLAG_PATTERN}))
            operations = tmp / 'operations.json'
            operations.write_text(json.dumps({
                'flag_pattern': FLAG_PATTERN, 'auto_submit_captured': False}))
            operations.chmod(0o600)
            subprocess.run(
                ['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                 '-keyout', str(tmp / 'tls.key'), '-out', str(tmp / 'tls.crt'),
                 '-subj', '/CN=localhost',
                 '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1',
                 '-days', '1'], check=True, capture_output=True)
            log = open(tmp / 'logs', 'w+')
            database = tmp / 'core.db'
            args = [str(BIN / 'hydra-core'), '--database', str(database),
                    '--key', str(tmp / 'jwt.pem'), '--socket', sock]

            def request(method, path, body=None, token=None):
                c = http.client.HTTPSConnection(
                    '127.0.0.1', http_port, timeout=10,
                    context=ssl._create_unverified_context())
                headers = {'Origin': origin}
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

            def good(method, path, body=None, token=None):
                status, reply, raw = request(method, path, body, token)
                self.assertIn(status, [200, 201, 202, 204], (status, raw[:300]))
                return reply

            # The access token lives 60 s; the browser phase can outlast it.
            # These API calls are the test's own setup/observation, never
            # the page's traffic path.
            api = {'token': None}

            def api_login():
                status, body, raw = request(
                    'POST', '/auth/login',
                    {'username': 'alice', 'password': 'test-password-1234'})
                self.assertEqual(status, 200, raw[:300])
                api['token'] = body['access_token']
                return api['token']

            def wait_for(fn, timeout=20.0):
                deadline = time.monotonic() + timeout
                while time.monotonic() < deadline:
                    value = fn()
                    if value:
                        return value
                    time.sleep(.05)
                return None

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

            def flow_by_port(service_id, client_port):
                page = good('GET', f'/traffic/flows?service_id={service_id}'
                                   '&newest_first=true&limit=100', token=api['token'])
                return next((f for f in page['items']
                             if (f.get('client') or {}).get('port') == client_port), None)

            def core_messages_have_flag(flow_id):
                status, _, raw = request(
                    'GET', f'/traffic/flows/{flow_id}/messages?limit=100',
                    token=api['token'])
                return status == 200 and FLAG.encode() in raw

            def core_history_status(flow_id):
                # Test-side observation of the same real gRPC GetFlowHistory
                # the page's socket selection uses; never page traffic.
                return request('GET', f'/traffic/flows/{flow_id}/history',
                               token=api['token'])[0]

            def wait_js(expression, seconds=20.0, context='DOM condition'):
                deadline = time.monotonic() + seconds
                while time.monotonic() < deadline:
                    if cdp.evaluate(expression):
                        return
                    cdp.pump()
                    time.sleep(.05)
                self.fail(f'timed out waiting for {context}; '
                          f'live status: {cdp.evaluate("document.querySelector(\'#traffic-live-status\')?.innerText")!r}; '
                          f'page: {cdp.evaluate("document.querySelector(\'#page\')?.innerText.slice(0,400)")!r}')

            def capture(name):
                raw = cdp.call('Page.captureScreenshot', {'format': 'png'})
                (screenshot_dir() / name).write_bytes(base64.b64decode(raw['data']))

            def live_notice():
                """The transient status line under #traffic-live-status.

                Core answers every failed request with the same generic text
                ("Streams request failed"), so an error frame's ``code`` is the
                only thing that names the failing request phase.
                """
                return cdp.evaluate(
                    "(()=>{const nodes=[...document.querySelectorAll("
                    "'#traffic-workspace p[role=\"status\"]')];"
                    "const own=nodes.find(p=>p.id==='traffic-live-status');"
                    "return nodes.filter(p=>p!==own).map(p=>p.innerText)"
                    ".join('\\n')})()") or ''

            def error_codes(index=0):
                return [frame.get('code') for frame in cdp.frames('Received', index)
                        if frame.get('type') == 'error']

            def choose_stream_view(label):
                current = cdp.evaluate(
                    "document.querySelector('[aria-label=\"Stream view\"]')"
                    "?.innerText || ''") or ''
                if label in current:
                    return
                cdp.click("document.querySelector('[aria-label=\"Stream view\"]')")
                wait_js("Boolean([...document.querySelectorAll('[role=option]')]"
                        f".find(o=>o.textContent.includes({js(label)})))",
                        context=f'{label} option')
                cdp.click("[...document.querySelectorAll('[role=option]')]"
                          f".find(o=>o.textContent.includes({js(label)}))")

            def assert_no_traffic_polls(index, context):
                polls = []
                for event in cdp.events[index:]:
                    if event.get('method') != 'Network.requestWillBeSent':
                        continue
                    params = event['params']
                    req = params.get('request') or {}
                    url = req.get('url') or ''
                    if req.get('method') != 'GET' or WS_PATH in url:
                        continue
                    if TRAFFIC_POLL.search(url):
                        polls.append(url.split('?', 1)[0])
                self.assertEqual(polls, [],
                                 f'{context}: the page polled traffic over HTTP instead of the socket')

            try:
                subprocess.run(args + ['create', 'user', 'alice', 'test-password-1234'],
                               check=True, stdout=log, stderr=log, timeout=10)
                processes.append(subprocess.Popen(
                    [str(BIN / 'hydra-proxy'), '--config', str(tmp / 'proxy.json')],
                    stdout=log, stderr=log))
                processes.append(subprocess.Popen(
                    args + ['serve', '--listen', f'127.0.0.1:{http_port}',
                            '--origin', origin,
                            '--tls-cert', str(tmp / 'tls.crt'),
                            '--tls-key', str(tmp / 'tls.key'),
                            '--operations', str(operations)],
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
                    self.fail('Core HTTPS did not become ready:\n' + log.read())

                # The served index must be the SPA this binary embedded at
                # compile time; a Core built before `frontend/dist` serves a
                # stale UI and every selector below would be misleading.
                c = http.client.HTTPSConnection(
                    '127.0.0.1', http_port, timeout=10,
                    context=ssl._create_unverified_context())
                c.request('GET', '/')
                r = c.getresponse()
                index_html = r.read().decode()
                c.close()
                self.assertEqual(r.status, 200)
                self.assertTrue(re.findall(r'<script[^>]+src="/assets/[^"]+"', index_html),
                                'Core did not serve the compiled Vite bundle')

                # Authenticate over the real API (same helper shape as the
                # protocol harness) to configure the two services.
                alice = api_login()

                bulk = good('POST', '/services', {
                    'expected_revision': 0, 'transport': 'tcp',
                    'listen_address': f'127.0.0.1:{bulk_port}',
                    'upstream_address': f'127.0.0.1:{upstream.getsockname()[1]}',
                    'protocol_id': 'utf8', 'enabled': True,
                    'retain_history': True}, alice)
                bulk_id = bulk['id']
                archive = good('POST', '/services', {
                    'expected_revision': 1, 'transport': 'tcp',
                    'listen_address': f'127.0.0.1:{archive_port}',
                    'upstream_address': f'127.0.0.1:{upstream.getsockname()[1]}',
                    'protocol_id': 'utf8', 'enabled': True,
                    'retain_history': True}, alice)
                archive_id = archive['id']
                self.assertTrue(wait_for(
                    lambda: good('GET', '/system/proxy', token=alice)
                    ['applied_services_revision'] == 2),
                    'Proxy did not apply both utf8 service snapshots')
                api_login()

                # 1. The search target is created first, on the other
                #    service, so it falls outside the newest-100 snapshot.
                old_payload = f'{EMOJI} archived {MARKER}\n'.encode('utf-8')
                old_sock, old_port, echoed = exchange(
                    old_payload, listen_port=archive_port)
                self.assertEqual(echoed, old_payload)
                old_flow = wait_for(lambda: flow_by_port(archive_id, old_port))
                self.assertIsNotNone(old_flow, 'Core did not index the old flow')
                old_id = old_flow['flow_id']

                # 2. More than one page of newer flows.
                filler_port = None
                for index in range(FILLERS):
                    payload = f'filler {index:03d} plain ascii line\n'.encode()
                    _, client_port, echoed = exchange(payload)
                    self.assertEqual(echoed, payload)
                    filler_port = client_port
                self.assertTrue(wait_for(lambda: flow_by_port(bulk_id, filler_port)),
                                'Core did not index the last filler flow')

                # 3. Literal-dot negative control (newest archive flow).
                decoy_payload = f'{DECOY} not a literal match\n'.encode()
                _, decoy_port, echoed = exchange(
                    decoy_payload, listen_port=archive_port)
                self.assertEqual(echoed, decoy_payload)
                self.assertIsNotNone(wait_for(lambda: flow_by_port(archive_id, decoy_port)))

                # 4. Still-open flow that the page will select.
                initial = f'{LIVE_MARKER}-initial\n'.encode()
                active_client, active_port, echoed = exchange(initial, hold=True)
                self.assertEqual(echoed, initial)
                active_flow = wait_for(lambda: flow_by_port(bulk_id, active_port))
                self.assertIsNotNone(active_flow, 'Core did not index the open flow')
                active_id = active_flow['flow_id']

                # ---- real browser, real login form ---------------------
                processes.append(subprocess.Popen(
                    [chrome, '--headless', '--no-sandbox', '--disable-gpu',
                     '--disable-dev-shm-usage', '--disable-background-networking',
                     '--disable-component-update', '--no-first-run',
                     '--ignore-certificate-errors',
                     f'--user-data-dir={tmp / "chrome"}',
                     f'--remote-debugging-port={debug}', 'about:blank'],
                    stdout=log, stderr=log))
                browser = processes[-1]
                startup_deadline = time.monotonic() + 60
                target = None
                while time.monotonic() < startup_deadline:
                    if browser.poll() is not None:
                        log.seek(0)
                        self.fail(f'{chrome} exited with status {browser.returncode} '
                                  f'before DevTools started:\n' + log.read())
                    try:
                        c = http.client.HTTPConnection('127.0.0.1', debug, timeout=2)
                        c.request('GET', '/json/list')
                        targets = json.loads(c.getresponse().read())
                        c.close()
                        target = next(t for t in targets if t['type'] == 'page')
                        break
                    except (OSError, StopIteration, http.client.HTTPException):
                        time.sleep(.05)
                else:
                    log.seek(0)
                    self.fail(f'{chrome} did not expose DevTools on port {debug}; '
                              f'exit status={browser.poll()}:\n' + log.read())

                page_target = target['id']
                cdp = RecordingCDP(target['webSocketDebuggerUrl'])
                cdp.call('Network.enable')
                cdp.call('Emulation.setDeviceMetricsOverride',
                         {'width': 1280, 'height': 900, 'deviceScaleFactor': 1,
                          'mobile': False})
                cdp.call('Page.navigate', {'url': origin + '/'})
                wait_js("document.querySelector('#login') && "
                        "!document.querySelector('#login').hidden", context='login form')
                self.assertTrue(cdp.evaluate(
                    "document.querySelector('#app')?.hasAttribute('data-v-app')"),
                    'Vue must mount the application')
                cdp.evaluate(
                    "for (const [name,value] of [['username','alice'],"
                    "['password','test-password-1234']]) {"
                    "const input=document.querySelector(`[name=${name}]`);"
                    "input.value=value;input.dispatchEvent(new Event('input',{bubbles:true}));}")
                cdp.evaluate("document.querySelector('#login-form').requestSubmit()")
                wait_js("document.querySelector('#console') && "
                        "!document.querySelector('#console').hidden", context='console')
                wait_js("Boolean(document.querySelector('#traffic-sessions'))",
                        context='traffic workspace')
                self.assertEqual(cdp.evaluate('location.hash'), '#/traffic')
                # Canonical primary predicate from ``browser.py``: the status
                # element's own text node is exactly `Live`. A prefix match
                # would also accept a stale/partial label; this does not.
                wait_js(TRAFFIC_LIVE_EXPRESSION, context='live socket ready')

                # ---- live label is the primary text node, counters a span ---
                # The server emits a `counters` frame right after the snapshot
                # (ws/subscribe.rs), so the span can appear a render after the
                # socket reports live: wait for the rendered frame instead of
                # reading a fixed slice.
                if not wait_until(
                        cdp,
                        lambda: (lambda facts: bool(facts) and len(facts['spans']) == 1)(
                            cdp.evaluate(LIVE_STATUS_FACTS)),
                        20.0, 'rendered live counters span'):
                    self.fail('the live status never rendered its counters child '
                              f'span; status facts: {cdp.evaluate(LIVE_STATUS_FACTS)!r}')
                live_facts = cdp.evaluate(LIVE_STATUS_FACTS)
                self.assertTrue(
                    cdp.evaluate(TRAFFIC_LIVE_EXPRESSION),
                    'the canonical predicate must hold on the real primary text '
                    f'node; status facts: {live_facts!r}')
                self.assertTrue(
                    live_facts['primaryIsText'],
                    "the primary status text must be the element's own text node, "
                    f'not a child element; status facts: {live_facts!r}')
                self.assertEqual(
                    live_facts['primary'], 'Live',
                    'the primary status text node must be exactly "Live", got '
                    f'{live_facts["primary"]!r}')
                self.assertNotEqual(
                    live_facts['text'], 'Live',
                    'the full status must carry the counters, so it cannot equal '
                    f'the primary label alone; status facts: {live_facts!r}')
                self.assertRegex(
                    live_facts['text'],
                    r'^Live · \d+ open · \d+ seen · \d+ messages$',
                    'the full status must be the Live label plus the server '
                    f'counters, got {live_facts["text"]!r}')
                self.assertRegex(
                    live_facts['spans'][0],
                    r'^ · \d+ open · \d+ seen · \d+ messages$',
                    'the counters child span must carry the server counters, got '
                    f'{live_facts["spans"]!r}')

                # ---- the page's own socket is the only transport -------
                created = cdp.since(0, 'Network.webSocketCreated')
                self.assertEqual(len(created), 1,
                                 f'expected exactly one page socket, got '
                                 f'{[p.get("url") for p in created]}')
                socket_url = created[0]['url']
                self.assertTrue(socket_url.endswith(WS_PATH),
                                f'page opened an unexpected socket: {socket_url}')
                self.assertTrue(socket_url.startswith('wss://'),
                                f'the page socket must be wss on an https origin: {socket_url}')
                self.assertIn('auth', cdp.types('Sent'))
                self.assertIn('subscribe', cdp.types('Sent'))
                self.assertIn('snapshot', cdp.types('Received'))

                # Everything from here on must be socket-driven. Mark the
                # event index now: bootstrap HTTP requests are allowed
                # before the first snapshot, none after it.
                bootstrap_index = len(cdp.events)

                # The old flow is genuinely outside the loaded page.
                rows = cdp.evaluate(
                    "document.querySelectorAll('[data-stream-card]').length")
                self.assertEqual(rows, FIRST_PAGE,
                                 f'the first page must be bounded to {FIRST_PAGE} rows')
                self.assertIsNone(
                    cdp.evaluate("document.querySelector("
                                 f"'[data-flow-id=\"{old_id}\"]')"),
                    'the old flow must not be in the first loaded page')

                # ---- selected active flow renders live bytes ----------
                cdp.click(f"document.querySelector('[data-flow-id=\"{active_id}\"]')")
                wait_js("document.querySelector('#traffic-stream')?.innerText"
                        ".includes('Live capture (partial, bounded)')",
                        context='live capture notice for the open flow')
                self.assertIn('retained tail',
                              cdp.evaluate("document.querySelector('#traffic-stream').innerText"),
                              'the open flow must render the retained live tail')
                self.assertIn(active_id,
                              [frame.get('flow_id') for frame in cdp.frames('Sent')
                               if frame.get('type') == 'select'],
                              'selecting the row must ride the page socket')
                self.assertIn('messages', cdp.types('Sent'),
                              'selecting the row must request messages over the socket')

                burst = f'{LIVE_MARKER}-burst\n'.encode()
                active_client.sendall(burst)
                self.assertEqual(read_exact(active_client, len(burst)), burst)

                terminal = ("(()=>{const t=document.querySelector('#traffic-terminal');"
                            "if(!t)return '';const tree=t.querySelector('.xterm-accessibility-tree');"
                            "return ((tree&&tree.innerText)||'')+'\\n'+((t.textContent)||'')})()")
                if not wait_until(
                        cdp, lambda: LIVE_MARKER + '-burst' in (cdp.evaluate(terminal) or ''),
                        20.0, 'live bytes'):
                    stream_now = cdp.evaluate(
                        "document.querySelector('#traffic-stream')?.innerText")
                    self.fail('the terminal did not render bytes written after selection; '
                              f'stream text: {stream_now!r}')
                self.assertTrue(
                    any(frame.get('type') == 'active_bytes'
                        for frame in cdp.frames('Received')),
                    'the page rendered live bytes without an active_bytes frame')
                # This selection is the fixture's healthy path: Core serves its
                # active history, so no failed request may leave a banner on
                # screen. Core's message is generic for every code, therefore a
                # failure here names the codes (request phases) it received.
                codes = error_codes(bootstrap_index)
                self.assertEqual(
                    codes, [], 'the live selection produced error frames '
                    f'{codes}; on-screen notice: {live_notice()!r}')
                self.assertNotIn(
                    'Streams request failed', live_notice(),
                    'a failed live request left its banner on screen while the '
                    'workspace was otherwise live')
                capture('streams-browser-live.png')

                # ---- later server answer carries the synthetic flag ---
                later_payload = (f'probe {HTML_PROBE} answer {FLAG} tail\n').encode()
                flow_index = len(cdp.events)
                later, later_port, echoed = exchange(
                    later_payload, listen_port=archive_port)
                self.assertEqual(echoed, later_payload)
                later_flow = wait_for(lambda: flow_by_port(archive_id, later_port))
                if later_flow is None:
                    self.fail('Core did not index the later flow')
                later_id = later_flow['flow_id']
                # Bounded wait for the actual frame; never a fixed sleep and
                # never a stale frame from an earlier flow.
                if not wait_until(
                        cdp,
                        lambda: any(
                            frame.get('type') == 'flow'
                            and (frame.get('event') or {}).get('flow_id')
                            == later_id
                            for frame in cdp.frames('Received', flow_index)),
                        10.0, 'live flow event for the later flow'):
                    self.fail('the closed later flow was not pushed live over the '
                              'socket within 10 s; received types since the exchange: '
                              f'{sorted(set(cdp.types("Received", flow_index)))}')
                self.assertTrue(wait_for(
                    lambda: (lambda f: f if f and f.get('closed') else None)(
                        flow_by_port(archive_id, later_port))),
                    'the later flow must be closed before it is searchable')

                # ---- global search: literal <mark> around the flag -----
                cdp.click("[...document.querySelectorAll('button')]"
                          ".find(b=>b.textContent==='Search all traffic')")
                wait_js("Boolean(document.querySelector('#traffic-global-search'))",
                        context='global search panel')
                cdp.evaluate("(()=>{const e=document.querySelector('#traffic-global-search');"
                             f"e.value={js(FLAG)};"
                             "e.dispatchEvent(new Event('input',{bubbles:true}));return true})()")

                def search_facts(query, direction=None):
                    return cdp.evaluate(
                        "(()=>{const lis=[...document.querySelectorAll("
                        "'[aria-label=\"Search results\"] li')];"
                        f"const hit=lis.find(li=>li.textContent.includes({js(query)})"
                        + (f" && li.textContent.includes({js(direction)})" if direction else '')
                        + ");if(!hit)return null;const p=hit.querySelector('p.font-mono');"
                        "if(!p)return null;"
                        "return {marks:[...p.querySelectorAll('mark')].map(m=>m.textContent),"
                        "snippet:p.textContent,"
                        "tags:[...p.querySelectorAll('*')].map(e=>e.tagName),"
                        "xss:Boolean(window.__xss)};})()")

                wait_until(cdp, lambda: search_facts(FLAG, 'Server → client') is not None,
                           25.0, 'flag search results')
                facts = search_facts(FLAG, 'Server → client')
                self.assertIsNotNone(
                    facts, 'global search found no server-answer hit for the synthetic flag')
                self.assertIn(FLAG, facts['marks'],
                              f'the flag must be a <mark> of exactly the flag text, '
                              f'got {facts["marks"]}')
                self.assertEqual(set(facts['tags']) - {'MARK'}, set(),
                                 f'the snippet must render literal text, got '
                                 f'{facts["tags"]}')
                self.assertIn('<b>literal</b>', facts['snippet'],
                              'the HTML probe must survive as literal text')
                self.assertFalse(facts['xss'],
                                 'the HTML probe executed: snippet is not inert text')
                capture('streams-browser-search-flag.png')

                # ---- global query finds the out-of-page old flow ------
                cdp.evaluate("(()=>{const e=document.querySelector('#traffic-global-search');"
                             f"e.value={js(MARKER)};"
                             "e.dispatchEvent(new Event('input',{bubbles:true}));return true})()")
                if not wait_until(cdp, lambda: cdp.evaluate(
                        "(()=>{const lis=[...document.querySelectorAll("
                        "'[aria-label=\"Search results\"] li')];"
                        f"return lis.some(li=>li.textContent.includes({js(MARKER)}))}})()"),
                        25.0, 'marker search results'):
                    self.fail('global search did not return the out-of-page flow for a '
                              'literal-dot query')
                self.assertFalse(cdp.evaluate(
                    "document.querySelector('[aria-label=\"Search results\"]')"
                    f"?.innerText.includes({js(DECOY)})"),
                    'a literal-dot query must not match the needleXone decoy')

                cdp.click("[...document.querySelectorAll("
                          "'[aria-label=\"Search results\"] li')]"
                          f".find(li=>li.textContent.includes({js(MARKER)}))"
                          ".querySelector('button')")
                # Core's `selected` metadata, not a fabricated row.
                if not wait_until(
                        cdp, lambda: any(f.get('type') == 'selected'
                                         and f.get('flow_id') == old_id
                                         for f in cdp.frames('Received')),
                        20.0, 'selected metadata'):
                    self.fail('opening a search hit did not resolve the flow over the '
                              'page socket')
                wait_js(f"document.querySelector('#traffic-stream')?.innerText"
                        f".includes({js(f'127.0.0.1:{old_port}')})",
                        context='real client endpoint from selected metadata')
                stream_text = cdp.evaluate(
                    "document.querySelector('#traffic-stream').innerText")
                self.assertIn(f'127.0.0.1:{old_port}', stream_text,
                              'the selected flow must show the real client endpoint')
                self.assertIn('Follow TCP stream', stream_text,
                              'the selected flow must show its real TCP transport')
                row_text = cdp.evaluate(
                    f"document.querySelector('[data-flow-id=\"{old_id}\"]')?.innerText || ''")
                self.assertIn('Closed', row_text,
                              f'the out-of-page flow must render its real Closed state, '
                              f'got {row_text!r}')
                self.assertIn('TCP', row_text,
                              f'the out-of-page flow must render its real transport, '
                              f'got {row_text!r}')
                capture('streams-browser-search-open.png')

                # ---- pause / resume on a genuinely hidden client ------
                # A real foreground tab hides this page; activating the original
                # target makes it visible again. No synthetic visibilitychange
                # is dispatched and the page lifecycle is never frozen.
                with self.subTest('pause/resume on real browser visibility'):
                    cdp.evaluate("window.__vis=[];"
                                 "document.addEventListener('visibilitychange',"
                                 "()=>window.__vis.push(document.visibilityState));true")
                    initial = cdp.evaluate('document.visibilityState')
                    if initial != 'visible':
                        self.skipTest(
                            'UNVERIFIED: the page starts with '
                            f'document.visibilityState={initial!r}; a hidden -> '
                            'visible transition cannot be exercised in this headless '
                            'Chromium, so pause/resume on real visibility is NOT proven')
                    pause_index = len(cdp.events)
                    other = None
                    try:
                        try:
                            other = cdp.call('Target.createTarget', {
                                'url': 'about:blank', 'background': False,
                                'newWindow': False})
                            cdp.call('Target.activateTarget',
                                     {'targetId': other['targetId']})
                        except AssertionError as exc:
                            self.skipTest(
                                'UNVERIFIED: headless Chromium could not open and '
                                'activate a foreground target to hide the page '
                                f'({exc}); real visibility pause/resume is NOT proven')
                        if not wait_until(
                                cdp,
                                lambda: cdp.evaluate('document.visibilityState') == 'hidden',
                                10.0, 'real hidden visibility'):
                            observed = cdp.evaluate('document.visibilityState')
                            capture('streams-browser-lifecycle-unverified.png')
                            self.skipTest(
                                'UNVERIFIED: activating a foreground target left '
                                f'document.visibilityState={observed!r}; this headless '
                                'Chromium does not expose real tab visibility, so the '
                                'page never becomes a genuinely hidden client and '
                                'pause is NOT proven')
                        if not wait_until(
                                cdp, lambda: 'pause' in cdp.types('Sent', pause_index),
                                10.0, 'pause frame'):
                            self.fail(
                                'the page became genuinely hidden '
                                '(document.visibilityState=hidden) but sent no pause '
                                'frame; sent types since hide: '
                                f'{sorted(set(cdp.types("Sent", pause_index)))}')
                        # Baseline BEFORE the page is made visible again: the
                        # reply to `resume` cannot be in this slice yet, so a
                        # match below is a frame the server produced for it,
                        # never one queued earlier or replayed from the log.
                        resume_index = len(cdp.events)
                        try:
                            cdp.call('Target.activateTarget',
                                     {'targetId': page_target})
                            cdp.call('Page.bringToFront')
                        except AssertionError as exc:
                            self.skipTest(
                                'UNVERIFIED: the browser could not re-activate the '
                                f'original target ({exc}); resume on real visibility '
                                'is NOT proven')
                        if not wait_until(
                                cdp,
                                lambda: cdp.evaluate('document.visibilityState') == 'visible',
                                10.0, 'real visible visibility'):
                            observed = cdp.evaluate('document.visibilityState')
                            capture('streams-browser-lifecycle-unverified.png')
                            self.skipTest(
                                'UNVERIFIED: activating the original target left '
                                f'document.visibilityState={observed!r}; this headless '
                                'Chromium keeps the page hidden, so resume on real '
                                'visibility is NOT proven')
                        if not wait_until(
                                cdp, lambda: 'resume' in cdp.types('Sent', pause_index),
                                10.0, 'resume frame'):
                            self.fail(
                                'the page became genuinely visible again '
                                '(document.visibilityState=visible) but sent no resume '
                                'frame; sent types since hide: '
                                f'{sorted(set(cdp.types("Sent", pause_index)))}')
                        visited = cdp.evaluate('window.__vis') or []
                        self.assertTrue(visited and visited[0] == 'hidden'
                                        and visited[-1] == 'visible',
                                        'the page must observe a real hidden -> '
                                        f'visible cycle, got {visited}')
                        self.assertEqual(cdp.evaluate('document.visibilityState'),
                                         'visible')
                        # Same canonical primary predicate as the initial wait:
                        # the resumed page must show the `Live` text node, not
                        # merely contain the substring somewhere in its status.
                        self.assertTrue(
                            cdp.evaluate(TRAFFIC_LIVE_EXPRESSION),
                            'after resume the primary status text node must be '
                            'exactly "Live"; status facts: '
                            f'{cdp.evaluate(LIVE_STATUS_FACTS)!r}')
                        # Core answers `resume` with a snapshot that echoes the
                        # resume request id (ws/commands.rs), so the bounded wait
                        # matches that exact reply. That counts only frames the
                        # server produced for this resume -- not frames queued
                        # before it and not an older snapshot.
                        resume_request = next((
                            frame.get('request_id')
                            for frame in reversed(cdp.frames('Sent', pause_index))
                            if frame.get('type') == 'resume'
                            and isinstance(frame.get('request_id'), str)), None)
                        self.assertIsNotNone(
                            resume_request,
                            'the page resumed without a request id; the server '
                            'reply cannot be matched to this resume')
                        if not wait_until(
                                cdp,
                                lambda: any(
                                    frame.get('type') == 'snapshot'
                                    and frame.get('request_id') == resume_request
                                    for frame in cdp.frames('Received', resume_index)),
                                10.0, 'snapshot answering the resume request'):
                            self.fail(
                                'the page received no snapshot for its resume '
                                f'request {resume_request!r} within 10 s; received '
                                'types since the resume: '
                                f'{sorted(set(cdp.types("Received", resume_index)))}')
                        capture('streams-browser-resumed.png')
                    finally:
                        if other is not None:
                            try:
                                cdp.call('Target.closeTarget',
                                         {'targetId': other['targetId']})
                            except AssertionError:
                                pass

                # ---- decoded server -> client response marks the flag -----
                # The upstream echoes the payload, so the flag exists in both
                # directions. The assertion targets the direction-2 (server ->
                # client) decoded card only: a client echo is not the server's
                # answer. This first selection is the healthy path; the
                # deterministic closed-history failure is exercised after it,
                # once the same flow's Proxy artifact is removed.
                # The closed later flow must have both a persisted Proxy history
                # and Core-ingested decoded SQL before the UI can select it.
                # These are real preconditions for the deterministic eviction
                # below, so wait for them instead of relying on a sleep.
                # Refresh the test-side access token before observing Core.
                api_login()
                history_dir = tmp / 'history'
                later_payload_zst = history_dir / f'hydra-history-{later_id}.zst'
                later_meta_json = history_dir / f'hydra-history-{later_id}.json'
                if not wait_for(lambda: later_payload_zst.exists()
                                and later_meta_json.exists()
                                and core_history_status(later_id) == 200, 20.0):
                    self.fail('the Proxy never persisted a readable closed later '
                              'flow history; history dir: '
                              f'{sorted(p.name for p in history_dir.glob("hydra-history-*"))}; '
                              f'history status: {core_history_status(later_id)}')
                if not wait_for(lambda: core_messages_have_flag(later_id), 20.0):
                    self.fail('Core had not ingested the later flow decoded messages '
                              'before the UI selected it')

                wait_js(f"Boolean(document.querySelector('[data-flow-id=\"{later_id}\"]'))",
                        context='later flag flow row')
                cdp.click(f"document.querySelector('[data-flow-id=\"{later_id}\"]')")
                wait_js("document.querySelector('#traffic-stream')?.innerText"
                        ".includes('decoded messages')", context='decoded flow header')
                choose_stream_view('Decoded conversation')
                wait_js("document.querySelector('[aria-label=\"Stream view\"]')"
                        "?.innerText.includes('Decoded')", context='decoded view')
                server_card = (
                    "(()=>{const root=document.querySelector('#traffic-stream');"
                    "if(!root)return null;"
                    "const card=[...root.querySelectorAll('[data-direction=\"2\"]')]"
                    ".find(el=>el.querySelector('mark'));"
                    "if(!card)return null;const pre=card.querySelector('pre')||card;"
                    "return {marks:[...card.querySelectorAll('mark')]"
                    ".map(m=>m.textContent),text:pre.textContent,"
                    "tags:[...pre.querySelectorAll('*')].map(e=>e.tagName),"
                    "xss:Boolean(window.__xss)};})()")
                if not wait_until(
                        cdp,
                        lambda: (lambda facts: bool(facts) and FLAG in facts['marks'])(
                            cdp.evaluate(server_card)),
                        15.0, 'server -> client decoded flag mark'):
                    self.fail('the decoded server -> client card did not mark the '
                              'flag within 15 s; card: '
                              f'{cdp.evaluate(server_card)!r}')
                facts = cdp.evaluate(server_card)
                self.assertIn(FLAG, facts['marks'],
                              'the server -> client decoded card must mark the '
                              f'flag text, got {facts["marks"]}')
                self.assertEqual(set(facts['tags']) - {'MARK'}, set(),
                                 'the decoded server response must render literal '
                                 f'text, got {facts["tags"]}')
                self.assertIn('<b>literal</b>', facts['text'],
                              'the HTML probe must survive as literal text in the '
                              'decoded server response')
                self.assertFalse(facts['xss'],
                                 'the HTML probe executed in the decoded server '
                                 'response: it is not inert text')
                if error_codes(bootstrap_index):
                    print('Live error codes observed (closed-flow selection may '
                          'report a missing retained tail in this fixture): '
                          f'{sorted(set(error_codes(bootstrap_index)))}', flush=True)
                # 1280x900 puts the server card below the client echo; bring the
                # mark into view so the capture shows the asserted direction.
                cdp.evaluate(
                    "(()=>{const root=document.querySelector('#traffic-stream');"
                    "const card=root&&[...root.querySelectorAll('[data-direction=\"2\"]')]"
                    ".find(el=>el.querySelector('mark'));"
                    "if(card)card.scrollIntoView({block:'center'});return true})()")
                capture('streams-browser-decoded-server-flag.png')

                # ---- deterministic closed-history loss (Proxy artifact only) --
                # The flow is closed, its real history is on disk and Core has
                # already ingested its decoded SQL. Remove only this fixture
                # flow's two history artifacts from the isolated temp Proxy
                # state; every other artifact must survive. A UI reselection
                # then makes the real gRPC GetFlowHistory answer NotFound,
                # which Core maps to the history-only `flow_not_found` error.
                api_login()
                self.assertTrue(core_messages_have_flag(later_id),
                                'Core decoded SQL must be present before eviction')
                metadata = json.loads(later_meta_json.read_text())
                self.assertEqual(metadata['flow_id'], later_id,
                                 'the history artifact key must match the fixture flow')
                self.assertEqual(metadata['service_id'], archive_id,
                                 'the history artifact must belong to the fixture service')
                self.assertGreater(later_payload_zst.stat().st_size, 0,
                                   'the fixture history payload must not be empty')
                before = {p.name for p in history_dir.iterdir()}
                self.assertIn(later_payload_zst.name, before)
                self.assertIn(later_meta_json.name, before)
                survivors = before - {later_payload_zst.name, later_meta_json.name}
                later_payload_zst.unlink()
                later_meta_json.unlink()
                self.assertFalse(later_payload_zst.exists() or later_meta_json.exists(),
                                 'the fixture flow history artifacts must be removed')
                self.assertEqual({p.name for p in history_dir.iterdir()}, survivors,
                                 'eviction must preserve every other Proxy history artifact')
                self.assertTrue(core_messages_have_flag(later_id),
                                'Proxy history eviction must not remove Core decoded SQL')
                if not wait_for(lambda: core_history_status(later_id) == 404, 10.0):
                    self.fail('the real gRPC GetFlowHistory did not answer NotFound '
                              'after the fixture history artifact was removed; '
                              f'status: {core_history_status(later_id)}')

                # Deselect through the UI by selecting the still-open flow,
                # then select the evicted closed flow again through the UI.
                cdp.click(f"document.querySelector('[data-flow-id=\"{active_id}\"]')")
                wait_js("document.querySelector('#traffic-stream')?.innerText"
                        ".includes('Live capture (partial, bounded)')",
                        context='live flow selected before the history reselection')
                evict_index = len(cdp.events)
                cdp.click(f"document.querySelector('[data-flow-id=\"{later_id}\"]')")

                select_request = None
                deadline = time.monotonic() + 20.0
                while time.monotonic() < deadline and select_request is None:
                    select_request = next((
                        frame.get('request_id')
                        for frame in cdp.frames('Sent', evict_index)
                        if frame.get('type') == 'select'
                        and frame.get('flow_id') == later_id
                        and isinstance(frame.get('request_id'), str)), None)
                    if select_request is None:
                        cdp.pump()
                        time.sleep(.05)
                self.assertIsNotNone(
                    select_request,
                    'the UI reselection did not send a correlated select request')

                if not wait_until(
                        cdp,
                        lambda: any(
                            frame.get('type') == 'error'
                            and frame.get('code') == 'flow_not_found'
                            and frame.get('request_id') == select_request
                            for frame in cdp.frames('Received', evict_index)),
                        20.0, 'flow_not_found answering the reselection'):
                    self.fail(
                        'the evicted closed flow did not produce a history-specific '
                        f'error for select request {select_request!r}; received: '
                        f'{[(f.get("type"), f.get("code"), f.get("request_id")) for f in cdp.frames("Received", evict_index)]}')

                error_codes_after = [frame.get('code') for frame in
                                     cdp.frames('Received', evict_index)
                                     if frame.get('type') == 'error']
                self.assertEqual(error_codes_after, ['flow_not_found'],
                                 'the reselection must report only the history code, '
                                 f'got {error_codes_after}')
                self.assertNotIn('flow_unavailable', error_codes_after,
                                 'a history eviction must not be reported as missing metadata')

                notice = live_notice()
                self.assertIn('No retained capture is stored for this session', notice,
                              'the history-specific retained-capture label must be visible')
                self.assertIn(f'(request {select_request})', notice,
                              'the visible notice must reference the failed request')
                self.assertIn('not a missing flow', notice,
                              'the notice must keep the flow record distinct from its history')
                self.assertNotIn(
                    'Streams request failed',
                    cdp.evaluate("document.querySelector('#page')?.innerText || ''"),
                    'the history mapping must not fall back to the generic banner')

                # Metadata and decoded SQL survive: Core still owns the flow
                # record and the already ingested messages.
                selected_frames = [frame for frame in cdp.frames('Received', evict_index)
                                   if frame.get('type') == 'selected'
                                   and frame.get('flow_id') == later_id]
                self.assertTrue(selected_frames,
                                'the reselection must still receive Core flow metadata')
                flow_meta = selected_frames[-1].get('flow') or {}
                self.assertEqual((flow_meta.get('client') or {}).get('port'), later_port,
                                 'the retained flow metadata must keep the real client endpoint')
                self.assertTrue(flow_meta.get('closed'),
                                'the retained flow metadata must stay Closed')
                wait_js(f"document.querySelector('#traffic-stream')?.innerText"
                        f".includes({js(f'127.0.0.1:{later_port}')})",
                        context='retained metadata after history eviction')
                self.assertIn(f'127.0.0.1:{later_port}',
                              cdp.evaluate("document.querySelector('#traffic-stream').innerText"),
                              'the reselected flow must keep its real endpoint')

                choose_stream_view('Decoded conversation')
                if not wait_until(
                        cdp,
                        lambda: (lambda facts: bool(facts) and FLAG in facts['marks'])(
                            cdp.evaluate(server_card)),
                        15.0, 'decoded server flag after history eviction'):
                    self.fail('the decoded server -> client flag did not survive the '
                              'Proxy history eviction; card: '
                              f'{cdp.evaluate(server_card)!r}')
                facts_after = cdp.evaluate(server_card)
                self.assertIn(FLAG, facts_after['marks'],
                              'Core decoded SQL must keep the server flag mark')
                self.assertFalse(facts_after['xss'],
                                 'the decoded server response must stay inert text')
                capture('streams-browser-history-partial.png')

                # ---- one socket, no HTTP traffic polling --------------
                self.assertEqual(len(cdp.since(0, 'Network.webSocketCreated')), 1,
                                 'pause/resume must not open a second page socket')
                assert_no_traffic_polls(bootstrap_index, 'after bootstrap')
                self.assertIn('search', cdp.types('Sent', bootstrap_index),
                              'global search must ride the page socket')
                print(f'Browser: {chrome} | socket: {socket_url}', flush=True)
            finally:
                if active_client is not None:
                    try:
                        active_client.close()
                    except OSError:
                        pass
                if cdp is not None:
                    try:
                        cdp.socket.close()
                    except OSError:
                        pass
                for process in reversed(processes):
                    if process.poll() is None:
                        process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                stop.set()
                upstream.close()
                if log is not None:
                    log.close()

if __name__ == '__main__':
    unittest.main()
