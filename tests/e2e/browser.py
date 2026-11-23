"""Headless Chromium smoke over Core's native HTTPS; standard library only."""
import base64
import gzip
import sqlite3
import http.client
import json
import os
import pathlib
import re
import shutil
import socket
import ssl
import struct
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from relay import BIN, port


# TrafficPage renders the live label as the status paragraph's own text node and
# appends counters in a separate child span ("Live · 0 open · 0 seen · 0
# messages"), so whole-element textContent never equals 'Live'. Match only the
# primary text node; it is 'Reconnecting live traffic…' while retrying, so this
# stays an exact connection-state check without accepting the counters suffix.
TRAFFIC_LIVE_EXPRESSION="document.querySelector('#traffic-live-status')?.firstChild?.textContent?.trim()==='Live'"


def browser_binary():
    # Hosted runners also install an unpinned Chromium snapshot. Match the
    # workflow's stable Chrome preference while retaining local Chromium support.
    return shutil.which('google-chrome') or shutil.which('chromium')


class BrowserSelection(unittest.TestCase):
    def test_prefers_stable_chrome_over_runner_snapshot(self):
        paths={'google-chrome':'/usr/bin/google-chrome','chromium':'/usr/bin/chromium'}
        with patch.object(shutil,'which',side_effect=paths.get):
            self.assertEqual(browser_binary(),paths['google-chrome'])

    def test_falls_back_to_chromium(self):
        with patch.object(shutil,'which',side_effect={'chromium':'/usr/bin/chromium'}.get):
            self.assertEqual(browser_binary(),'/usr/bin/chromium')


def rename_dist_preserving_parent_mtime(source, destination):
    # This smoke runs with no concurrent frontend changes. Preserve only the
    # directory timestamp changed by our temporary asset-availability probe.
    assert source.parent == destination.parent
    parent = source.parent
    before = parent.stat()
    source.rename(destination)
    os.utime(parent, ns=(before.st_atime_ns, before.st_mtime_ns))


class CDP:
    def __init__(self, address):
        from urllib.parse import urlsplit
        url = urlsplit(address)
        self.socket = socket.create_connection((url.hostname, url.port), timeout=5)
        key = base64.b64encode(os.urandom(16)).decode()
        self.socket.sendall(f'GET {url.path} HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n'.encode())
        headers = b''
        while not headers.endswith(b'\r\n\r\n'):
            headers += self.socket.recv(1)
        assert b'101' in headers, headers
        self.sequence = 0
    def read(self, count):
        value = b''
        while len(value) < count:
            chunk = self.socket.recv(count - len(value))
            if not chunk:
                raise EOFError('browser disconnected')
            value += chunk
        return value
    def call(self, method, params=None):
        self.sequence += 1
        payload = json.dumps({'id': self.sequence, 'method': method, 'params': params or {}}).encode()
        n = len(payload)
        header = bytes([0x81, 0x80 | n]) if n < 126 else bytes([0x81, 0x80 | 126]) + struct.pack('!H', n)
        mask = os.urandom(4)
        self.socket.sendall(header + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(payload)))
        while True:
            start = self.read(2)
            length = start[1] & 127
            if length == 126:
                length = struct.unpack('!H', self.read(2))[0]
            elif length == 127:
                length = struct.unpack('!Q', self.read(8))[0]
            assert length < 4 * 1024 * 1024
            result = json.loads(self.read(length))
            if result.get('id') == self.sequence:
                assert 'error' not in result, result
                return result.get('result', {})
    def evaluate(self, source):
        result = self.call('Runtime.evaluate', {'expression': source, 'returnByValue': True, 'awaitPromise': True})
        assert 'exceptionDetails' not in result, result
        return result.get('result', {}).get('value')
    def click(self, expression):
        # Select menus can be taller than their scroll viewport. Wait for the
        # target to finish scrolling before calculating its mouse coordinates;
        # otherwise a smooth-scroll race can click a neighboring option. A live
        # WebSocket frame can also replace the surrounding panel between the
        # coordinate read and the dispatched event, which silently loses the
        # click (reka-ui keeps aria-expanded=false); re-resolve the element and
        # confirm it is the hit-test target before dispatching.
        for _ in range(40):
            point=self.evaluate(f'(async()=>{{const e=({expression});if(!e)return null;e.scrollIntoView({{block:"center",behavior:"instant"}});await new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)));const r=e.getBoundingClientRect();const x=r.x+r.width/2,y=r.y+r.height/2;const hit=document.elementFromPoint(x,y);return {{x,y,ok:Boolean(hit)&&(hit===e||e.contains(hit)||hit.contains(e))}}}})()')
            if point and point['ok']:break
            time.sleep(.05)
        else:raise AssertionError(f'click target never became hit-testable: {expression}')
        for kind in ['mousePressed','mouseReleased']:
            self.call('Input.dispatchMouseEvent',{'type':kind,'x':point['x'],'y':point['y'],'button':'left','clickCount':1})


class Browser(unittest.TestCase):
    @unittest.skipUnless(browser_binary(), 'Chromium or Google Chrome is required')
    def test_native_https_login_navigation_logout(self):
        chrome = browser_binary()
        processes = []
        upstream = None
        retry_backend = None
        occupied = None
        retry_backend_stop = threading.Event()
        with tempfile.TemporaryDirectory(prefix='hydra-browser-') as directory:
            tmp = pathlib.Path(directory)
            # Screenshots are debug artifacts of this smoke test. Keep them inside
            # the test-owned fixture directory instead of shared /tmp paths.
            artifacts = tmp / 'artifacts'
            artifacts.mkdir()
            ap, debug = port(), port()
            sock = str(tmp / 'proxy.sock')
            (tmp / 'proxy.json').write_text(json.dumps({'socket': sock, 'state_file': str(tmp / 'saved.json'), 'flag_pattern':'FLAG[0-9]{6}','limits':{'udp_idle_ms':250}}))
            subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(tmp/'tls.key'),'-out',str(tmp/'tls.crt'),'-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost,IP:127.0.0.1','-days','1'],check=True,capture_output=True)
            (tmp/'operations.json').write_text(json.dumps({'flag_pattern':'FLAG[0-9]{6}','auto_submit_captured':False}))
            (tmp/'operations.json').chmod(0o600)
            args = [str(BIN/'hydra-core'),'--database',str(tmp/'db'),'--key',str(tmp/'jwt.pem'),'--socket',sock]
            log = open(tmp/'process.log','w+')
            cdp=None
            dist = pathlib.Path(__file__).resolve().parents[2] / "frontend" / "dist"
            hidden_dist = None
            try:
                subprocess.run(args+['create','user','alice','test-password-1234'],check=True,stdout=log,stderr=log,timeout=10)
                processes.append(subprocess.Popen([str(BIN/'hydra-proxy'),'--config',str(tmp/'proxy.json')],stdout=log,stderr=log))
                if os.environ.get('HYDRA_VERIFY_EMBEDDED') == '1':
                    self.assertTrue(dist.is_dir(), 'Build frontend/dist before this smoke test')
                    hidden_dist = dist.with_name(f'dist.browser-hidden-{os.getpid()}')
                    rename_dist_preserving_parent_mtime(dist, hidden_dist)
                processes.append(subprocess.Popen(args+['serve','--listen',f'127.0.0.1:{ap}','--origin',f'https://localhost:{ap}','--tls-cert',str(tmp/'tls.crt'),'--tls-key',str(tmp/'tls.key'),'--operations',str(tmp/'operations.json')],stdout=log,stderr=log))
                for _ in range(100):
                    try:
                        c=http.client.HTTPSConnection('127.0.0.1',ap,context=ssl._create_unverified_context(),timeout=2)
                        c.request('GET','/');r=c.getresponse();index=r.read().decode();c.close()
                        if r.status==200:
                            break
                    except (OSError,http.client.HTTPException):
                        pass
                    time.sleep(.05)
                else:
                    log.seek(0);self.fail(log.read())
                scripts = re.findall(r'<script[^>]+src="([^"]+)"', index)
                self.assertTrue(scripts, 'Core must serve the Vite compiled module')
                for script in scripts:
                    self.assertRegex(script, r'^/assets/[^/]+-[A-Za-z0-9_-]+\.js$')
                    c=http.client.HTTPSConnection('127.0.0.1',ap,context=ssl._create_unverified_context(),timeout=2)
                    c.request('GET',script);r=c.getresponse();bundle=r.read();c.close()
                    self.assertEqual(r.status,200)
                    self.assertIn('javascript',r.getheader('Content-Type'))
                    self.assertEqual(r.getheader('X-Content-Type-Options'),'nosniff')
                    self.assertIn('immutable',r.getheader('Cache-Control'))
                    self.assertGreater(len(bundle),1000)
                print(f'Browser smoke executable: {chrome}',flush=True)
                browser=subprocess.Popen([chrome,'--headless','--no-sandbox','--disable-gpu','--disable-dev-shm-usage','--disable-background-networking','--disable-component-update','--no-first-run','--ignore-certificate-errors',f'--user-data-dir={tmp / "chrome"}',f'--remote-debugging-port={debug}','about:blank'],stdout=log,stderr=log)
                processes.append(browser)
                # GitHub's shared hosted runners can take several seconds to start Chrome.
                startup_deadline = time.monotonic() + 60
                while time.monotonic() < startup_deadline:
                    if browser.poll() is not None:
                        log.seek(0);self.fail(f'{chrome} exited with status {browser.returncode} before DevTools started:\n'+log.read())
                    try:
                        c=http.client.HTTPConnection('127.0.0.1',debug,timeout=2);c.request('GET','/json/list');targets=json.loads(c.getresponse().read());c.close()
                        target=next(t for t in targets if t['type']=='page');break
                    except (OSError,StopIteration,http.client.HTTPException):
                        time.sleep(.05)
                else:
                    log.seek(0);self.fail(f'{chrome} did not expose DevTools on port {debug}; exit status={browser.poll()}:\n'+log.read())
                cdp=CDP(target['webSocketDebuggerUrl'])
                cdp.call('Emulation.setDeviceMetricsOverride',{'width':1280,'height':900,'deviceScaleFactor':1,'mobile':False})
                cdp.call('Page.navigate',{'url':f'https://localhost:{ap}/'})
                def wait(expression, seconds=5):
                    for _ in range(int(seconds*20)):
                        if cdp.evaluate(expression):
                            return
                        time.sleep(.05)
                    self.fail(cdp.evaluate("'Controls: '+JSON.stringify([...document.querySelectorAll('[role=combobox]')].map(e=>e.outerHTML))+'\\n'+document.body.innerText+'\\nEditors: '+[...document.querySelectorAll('[role=textbox]')].map(e=>e.outerHTML).join('\\n')+'\\nTextareas: '+[...document.querySelectorAll('textarea')].map(e=>e.outerHTML).join('\\n')"))
                def open_select(trigger,label,seconds=8):
                    # reka-ui Select opens on the trigger's pointerdown; a
                    # concurrent panel re-render can swallow a single click and
                    # leave aria-expanded=false, so retry the trigger until the
                    # option list is actually open (the caller still asserts and
                    # clicks the exact option).
                    option=f"[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(e=>e.textContent.trim()==={json.dumps(label)})"
                    deadline=time.time()+seconds
                    while time.time()<deadline:
                        if cdp.evaluate(option):return
                        if not cdp.evaluate("Boolean(document.querySelector('[role=listbox][data-state=open]'))"):
                            cdp.click(trigger)
                        time.sleep(.15)
                    self.fail(f'select option {label!r} never opened from {trigger}')
                def replace_editor(selector, value):
                    cdp.click(f"document.querySelector({json.dumps(selector)})")
                    cdp.call('Input.dispatchKeyEvent',{'type':'keyDown','key':'Control','code':'ControlLeft','modifiers':2})
                    cdp.call('Input.dispatchKeyEvent',{'type':'keyDown','key':'a','code':'KeyA','modifiers':2})
                    cdp.call('Input.dispatchKeyEvent',{'type':'keyUp','key':'a','code':'KeyA','modifiers':2})
                    cdp.call('Input.dispatchKeyEvent',{'type':'keyUp','key':'Control','code':'ControlLeft'})
                    cdp.call('Input.insertText',{'text':value})
                wait("document.querySelector('#login') && !document.querySelector('#login').hidden")
                self.assertTrue(cdp.evaluate("document.querySelector('#app')?.hasAttribute('data-v-app')"), "Vue must mount the application")
                # Theme works before login and persists through a complete reload.
                self.assertEqual(cdp.evaluate('document.documentElement.dataset.theme'), 'dark')
                cdp.click("document.querySelector('#theme-toggle')")
                wait("document.documentElement.dataset.theme==='oled'")
                self.assertEqual(cdp.evaluate('getComputedStyle(document.body).backgroundColor'), 'rgb(0, 0, 0)')
                original=cdp.evaluate('performance.timeOrigin')
                cdp.call('Page.reload')
                wait(f"performance.timeOrigin!=={original} && document.querySelector('#login') && document.documentElement.dataset.theme==='oled'")
                cdp.evaluate("for (const [name,value] of [['username','alice'],['password','test-password-1234']]) { const input=document.querySelector(`[name=${name}]`);input.value=value;input.dispatchEvent(new Event('input',{bubbles:true})); }")
                cdp.evaluate("document.querySelector('#login-form').requestSubmit()")
                wait("document.querySelector('#console') && !document.querySelector('#console').hidden")
                wait("Boolean(document.querySelector('#traffic-sessions') && document.querySelector('#traffic-service'))")
                self.assertEqual(cdp.evaluate('location.hash'), '#/traffic')
                original=cdp.evaluate('performance.timeOrigin')
                cdp.call('Page.reload')
                wait(f"performance.timeOrigin!=={original} && document.querySelector('#console') && !document.querySelector('#console').hidden")
                # Real Vue bindings and mutations, with a loopback-only service and no workers.
                cdp.evaluate("[...document.querySelectorAll('nav button')].find(b=>b.textContent==='Services').click()")
                wait("[...document.querySelectorAll('#page button')].some(b=>b.textContent==='Add service' && !b.disabled)")
                cdp.evaluate("[...document.querySelectorAll('#page button')].find(b=>b.textContent==='Add service').click()")
                wait("document.querySelector('#service-form') !== null")
                self.assertEqual(cdp.evaluate("document.querySelector('#service-forwarding').getAttribute('aria-checked')"), 'false')
                cdp.evaluate("document.querySelector('#service-forwarding').click()")
                wait("document.querySelector('#forward-pattern') !== null")
                self.assertEqual(cdp.evaluate("document.querySelector('#forward-pattern').value"), '[A-Z0-9]{31}=')
                self.assertIn('TCP message framing',cdp.evaluate("document.querySelector('#service-form').innerText"))
                cdp.evaluate("document.querySelector('#service-forwarding').click()")
                wait("document.querySelector('#forward-pattern') === null")
                upstream=socket.socket();upstream.bind(('127.0.0.1',0));upstream.listen();upstream.settimeout(10)
                http_body=b'{"message":"<b>hello</b>","items":[1,2]}'
                release_response=threading.Event()
                def reply():
                    try:
                        connection,_=upstream.accept()
                        with connection:
                            connection.settimeout(3);data=b''
                            while b'\r\n\r\n' not in data and len(data)<8192:
                                chunk=connection.recv(4096)
                                if not chunk:return
                                data+=chunk
                            release_response.wait(5)
                            connection.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: '+str(len(http_body)).encode()+b'\r\nContent-Type: application/json\r\nX-Reply: visible\r\nConnection: close\r\n\r\n'+http_body)
                    except OSError:
                        pass
                threading.Thread(target=reply,daemon=True).start()
                service = {'name':'Browser HTTP','transport':'tcp','listen_address':f'127.0.0.1:{port()}','upstream_address':f'127.0.0.1:{upstream.getsockname()[1]}','protocol_id':'http','enabled':True}
                def fill_service(value):
                    wait("document.querySelector('#service-save') && !document.querySelector('#service-save').disabled")
                    value.setdefault('name',value['listen_address'])
                    for field,key in [('service-name','name'),('service-listener','listen_address'),('service-backend','upstream_address'),('service-profile','protocol_id')]:
                        cdp.evaluate(f"{{const e=document.querySelector('#{field}');e.value={json.dumps(value[key])};e.dispatchEvent(new Event('input',{{bubbles:true}}));}}")
                    cdp.evaluate("document.querySelector('#service-form').requestSubmit()")
                    wait(f"document.querySelector('#page').innerText.includes({json.dumps(value['listen_address'])}) && !document.querySelector('#service-form')")
                cdp.click("document.querySelector('#service-protection')")
                wait("document.querySelector('#protection-max_active_flows')?.value==='128'")
                fill_service(service)
                # A second real configured service has no sessions, to verify dropdown filtering.
                cdp.evaluate("[...document.querySelectorAll('#page button')].find(b=>b.textContent==='Add service').click()")
                wait("document.querySelector('#service-form') !== null")
                other={**service,'name':'Unused service','listen_address':f'127.0.0.1:{port()}'}
                fill_service(other)
                self.assertEqual(cdp.evaluate("[...document.querySelector('#page table').querySelectorAll('thead th')].map(e=>e.textContent)"),['Name','Color','Transport','Listener','Destination','Decoder','Buttons'])
                self.assertIn('Browser HTTP',cdp.evaluate("document.querySelector('#page table').innerText"))
                self.assertEqual(cdp.evaluate("document.querySelector('#page table [aria-label^=\"Set color for\"]').textContent.trim()"),'')
                cdp.click("[...document.querySelectorAll('#page tr')].find(r=>r.innerText.includes('Browser HTTP')).querySelectorAll('button')[2]")
                wait("Boolean(document.querySelector('#service-forwarding-settings'))")
                self.assertEqual(cdp.evaluate("document.querySelector('#service-forwarding').getAttribute('aria-checked')"),'false')
                self.assertIn('TCP mirroring',cdp.evaluate("document.querySelector('#service-forwarding-settings').textContent"))
                cdp.call('Input.dispatchKeyEvent',{'type':'keyDown','key':'Escape','code':'Escape','windowsVirtualKeyCode':27})
                cdp.call('Input.dispatchKeyEvent',{'type':'keyUp','key':'Escape','code':'Escape','windowsVirtualKeyCode':27})
                wait("!document.querySelector('#service-form')")
                (artifacts / 'hydra-services-desktop.png').write_bytes(base64.b64decode(cdp.call('Page.captureScreenshot',{'format':'png'})['data']))
                wait("document.querySelectorAll('[aria-label=\"Live service metrics\"] svg').length>=4", seconds=10)
                wait("document.querySelector('[aria-label=\"Live service metrics\"]')?.innerText.includes('Live · every 2 seconds')", seconds=10)
                # Hold a loopback port through a real listener bind failure,
                # then exercise the Services page Retry action after freeing it.
                retry_backend=socket.socket();retry_backend.bind(('127.0.0.1',0));retry_backend.listen();retry_backend.settimeout(.2)
                def retry_echo():
                    while not retry_backend_stop.is_set():
                        try:peer,_=retry_backend.accept()
                        except socket.timeout:continue
                        except OSError:return
                        def echo(peer):
                            with peer:
                                try:
                                    while data:=peer.recv(4096):peer.sendall(data)
                                except OSError:pass
                        threading.Thread(target=echo,args=(peer,),daemon=True).start()
                threading.Thread(target=retry_echo,daemon=True).start()
                occupied=socket.socket();occupied.bind(('127.0.0.1',0));occupied.listen()
                retry_service={'name':'Browser retry','transport':'tcp','listen_address':f'127.0.0.1:{occupied.getsockname()[1]}','upstream_address':f'127.0.0.1:{retry_backend.getsockname()[1]}','protocol_id':'raw','enabled':True}
                cdp.evaluate("[...document.querySelectorAll('#page button')].find(b=>b.textContent==='Add service').click()")
                wait("document.querySelector('#service-form') !== null")
                fill_service(retry_service)
                error_deadline=time.monotonic()+8
                while time.monotonic()<error_deadline:
                    cdp.evaluate("[...document.querySelectorAll('#page button')].find(b=>b.textContent==='Refresh')?.click()")
                    if cdp.evaluate("document.querySelector('#page').innerText.includes('os error 98')"):break
                    time.sleep(.1)
                else:self.fail('Browser retry listener did not report the held address in use')
                self.assertIn('free the listener address, then retry.',cdp.evaluate("document.querySelector('#page').innerText"))
                self.assertTrue(cdp.evaluate("Boolean(document.querySelector('[aria-label=\"Retry listener for Browser retry\"]'))"))
                wait("!document.querySelector('#page').innerText.includes('Loading services…') && !document.querySelector('[aria-label=\"Retry listener for Browser retry\"]').disabled")
                retry_revision=cdp.evaluate("Number(document.querySelector('#page').innerText.match(/Revision (\\d+)/)?.[1])")
                occupied.close()
                cdp.click("document.querySelector('[aria-label=\"Retry listener for Browser retry\"]')")
                wait(f"document.querySelector('#page').innerText.includes('Retry requested for revision {retry_revision}')")
                wait("document.querySelector('#page').innerText.includes('Pending listener and filter changes were applied at revision')",seconds=8)
                with socket.create_connection(('127.0.0.1',int(retry_service['listen_address'].split(':')[1])),timeout=3) as client:
                    client.sendall(b'ui-retry-bound');self.assertEqual(client.recv(64),b'ui-retry-bound')
                with sqlite3.connect(tmp/'db') as database:
                    self.assertEqual(database.execute('SELECT revision FROM control_state WHERE id=1').fetchone()[0],retry_revision)
                    current_services=json.loads(database.execute('SELECT services FROM control_state WHERE id=1').fetchone()[0])
                    retry_id=next(item['id'] for item in current_services if item['listen_address']==retry_service['listen_address'])
                    audit=database.execute("SELECT actor_id,target,metadata FROM audit WHERE action='service.retry' AND target=?",[retry_id]).fetchone()
                    self.assertEqual(audit[0],database.execute("SELECT id FROM users WHERE username='alice'").fetchone()[0])
                    self.assertEqual(audit[1],retry_id)
                    self.assertEqual(json.loads(audit[2])['expected_revision'],retry_revision)
                # Remove only this test service, leaving the original browser
                # fixtures and their expected service-selection counts intact.
                cdp.evaluate('window.confirm=()=>true')
                cdp.click("[...document.querySelectorAll('#page tr')].find(row=>row.innerText.includes('Browser retry')).querySelectorAll('button') && [...[...document.querySelectorAll('#page tr')].find(row=>row.innerText.includes('Browser retry')).querySelectorAll('button')].find(button=>button.textContent==='Delete')")
                wait("![...document.querySelectorAll('#page tr')].some(row=>row.innerText.includes('Browser retry'))")
                delete_revision=retry_revision+1
                apply_deadline=time.monotonic()+8
                while time.monotonic()<apply_deadline:
                    cdp.evaluate("[...document.querySelectorAll('#page button')].find(button=>button.textContent==='Refresh' && !button.disabled)?.click()")
                    if cdp.evaluate(f"document.querySelector('#page').innerText.includes('Revision {delete_revision} · applied · applied {delete_revision}')"):
                        break
                    time.sleep(.2)
                else:self.fail('Deleting the retry fixture did not apply after refreshing the Services page: '+cdp.evaluate("document.querySelector('#page').innerText"))
                cdp.click("[...document.querySelectorAll('#page tr')].find(e=>e.innerText.includes("+json.dumps(service['listen_address'])+")).querySelector('[aria-label^=\"Set color for\"]')")
                wait("document.querySelectorAll('[role=dialog] [aria-label$=\"service color\"][aria-pressed]').length===12")
                cdp.click("document.querySelector('[aria-label=\"Cyan service color\"]')")
                cdp.click("[...document.querySelectorAll('[role=dialog] button')].find(e=>e.textContent==='Save color')")
                wait("!document.querySelector('[role=dialog]')")
                cdp.evaluate("[...document.querySelectorAll('nav button')].find(b=>b.textContent==='Traffic').click()")
                wait(TRAFFIC_LIVE_EXPRESSION)
                cdp.click("document.querySelector('#traffic-service')")
                wait("document.querySelectorAll('[role=listbox][data-state=open] [role=option]').length===3")
                cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==="+json.dumps(service['name'])+")")
                for _ in range(100):
                    try:
                        connection=socket.create_connection(('127.0.0.1',int(service['listen_address'].split(':')[1])),timeout=2)
                        break
                    except OSError:time.sleep(.05)
                else:self.fail('Fixture listener did not apply')
                with connection:
                    connection.sendall(b'GET /browser-stream?tag=one&tag=two HTTP/1.1\r\nHost: browser-fixture\r\nX-Fixture-Flag: FLAG123456\r\nX-Visible: one\r\nX-Visible: two\r\nConnection: close\r\n\r\n')
                    wait("Boolean(document.querySelector('#traffic-stream [data-direction=\"1\"]'))")
                    release_response.set()
                    self.assertIn(b'200 OK',connection.recv(4096))
                wait("Boolean(document.querySelector('#traffic-sessions [data-flow-id]') && document.querySelector('#traffic-stream [data-message-id]'))")
                self.assertIn('/browser-stream',cdp.evaluate("document.querySelector('#traffic-stream').innerText"))
                wait("Boolean(document.querySelector('#traffic-sessions [data-has-flag=\"true\"]'))")
                self.assertIn('Flag',cdp.evaluate("document.querySelector('#traffic-sessions [data-has-flag=\"true\"]').innerText"))
                self.assertTrue(cdp.evaluate("document.querySelector('#traffic-sessions [data-flow-id]').closest('[data-stream-card]').style.borderLeftColor.length>0"))
                self.assertEqual(cdp.evaluate("document.querySelector('#traffic-sessions [data-flow-id]').closest('[data-stream-card]').style.borderLeftColor"),'rgb(102, 217, 232)')
                self.assertTrue(cdp.evaluate("Boolean(document.querySelector('#traffic-sessions [aria-label^=\"Select stream for filter\"]').closest('[data-stream-card]'))"))
                self.assertEqual(cdp.evaluate("document.querySelector('#traffic-show-blocked').getAttribute('aria-checked')"), 'false')
                self.assertEqual(cdp.evaluate("[...document.querySelectorAll('#traffic-stream [aria-label=\"HTTP headers\"] tr')].filter(r=>r.cells[0].textContent==='X-Visible').map(r=>r.cells[1].textContent)"),[' one',' two'])
                self.assertEqual(cdp.evaluate("[...document.querySelectorAll('#traffic-stream [aria-label=\"HTTP query parameters\"] tr')].map(r=>r.cells[1].textContent)"),['one','two'])
                self.assertTrue(cdp.evaluate("document.querySelector('#traffic-stream [data-direction=\"1\"]')!==null"))
                wait("Boolean(document.querySelector('#traffic-stream [data-direction=\"2\"]'))")
                self.assertIn('200 OK',cdp.evaluate("document.querySelector('#traffic-stream [data-direction=\"2\"] [data-http-start-line]').textContent"))
                self.assertIn(' visible',cdp.evaluate("[...document.querySelectorAll('#traffic-stream [aria-label=\"HTTP headers\"] tr')].find(r=>r.cells[0].textContent==='X-Reply').cells[1].textContent"))
                self.assertIn('"message": "<b>hello</b>"',cdp.evaluate("document.querySelector('#traffic-stream [data-direction=\"2\"] [data-http-body]').textContent"))
                self.assertFalse(cdp.evaluate("Boolean(document.querySelector('#traffic-stream [data-http-body] b'))"))
                cdp.click("[...document.querySelectorAll('#traffic-stream button')].find(b=>b.textContent==='Text')")
                self.assertEqual(cdp.evaluate("document.querySelector('#traffic-stream [data-direction=\"2\"] [data-http-body]').textContent"),http_body.decode())
                (artifacts / 'hydra-http-view-desktop.png').write_bytes(base64.b64decode(cdp.call('Page.captureScreenshot',{'format':'png'})['data']))
                cdp.click("document.querySelector('#traffic-sessions [aria-label=\"Save stream in favourites\"]')")
                wait("document.querySelector('#traffic-sessions [aria-label=\"Remove stream from favourites\"]')?.getAttribute('aria-pressed')==='true'")
                cdp.click("document.querySelector('#traffic-favourites-only')")
                wait("document.querySelector('#traffic-favourites-only')?.getAttribute('aria-checked')==='true' && document.querySelectorAll('#traffic-sessions [data-flow-id]').length===1")
                wait("[...document.querySelectorAll('#traffic-sessions button')].some(b=>b.textContent.trim()==='Refresh' && !b.disabled)")
                cdp.evaluate("[...document.querySelectorAll('#traffic-sessions button')].find(b=>b.textContent.trim()==='Refresh').click()")
                wait("Boolean(document.querySelector('#traffic-sessions [aria-label=\"Remove stream from favourites\"]'))")
                self.assertIn('Saved to favourites', cdp.evaluate("document.querySelector('#traffic-stream').innerText"))
                cdp.click("document.querySelector('#traffic-favourites-only')")
                wait("Boolean(document.querySelector('#traffic-sessions [data-flow-id]'))")
                cdp.evaluate("document.querySelector('#traffic-sessions [data-flow-id]').click()")
                wait("document.querySelector('#traffic-sessions [aria-pressed=true]')!==null")
                self.assertTrue(cdp.evaluate("(()=>{const l=document.querySelector('#traffic-sessions').getBoundingClientRect(),r=document.querySelector('#traffic-stream').getBoundingClientRect();return r.left>=l.right-1})()"))
                initial_width=cdp.evaluate("document.querySelector('#traffic-sessions').getBoundingClientRect().width")
                cdp.evaluate("document.querySelector('[aria-label=\"Resize streams list\"]').focus()")
                cdp.call('Input.dispatchKeyEvent',{'type':'keyDown','key':'ArrowRight','code':'ArrowRight','windowsVirtualKeyCode':39})
                cdp.call('Input.dispatchKeyEvent',{'type':'keyUp','key':'ArrowRight','code':'ArrowRight','windowsVirtualKeyCode':39})
                wait(f"document.querySelector('#traffic-sessions').getBoundingClientRect().width>{initial_width+5}")
                resized_width=cdp.evaluate("document.querySelector('#traffic-sessions').getBoundingClientRect().width")
                cdp.click("document.querySelector('#traffic-expand')")
                wait("document.querySelector('#traffic-expand').getAttribute('aria-pressed')==='true'")
                wait("document.querySelector('#traffic-workspace').getBoundingClientRect().width===innerWidth")
                wait(f"Math.abs(document.querySelector('#traffic-sessions').getBoundingClientRect().width-{resized_width})<2")
                self.assertAlmostEqual(cdp.evaluate("document.querySelector('#traffic-sessions').getBoundingClientRect().width"),resized_width,delta=2)
                self.assertTrue(cdp.evaluate("(()=>{const r=document.querySelector('#traffic-workspace').getBoundingClientRect();return r.x===0&&r.y===0&&r.height===innerHeight})()"))
                # Wide panes align chronological request/response blocks left/right.
                wait("(()=>{const l=document.querySelector('#traffic-stream [data-direction=\"1\"]')?.getBoundingClientRect(),r=document.querySelector('#traffic-stream [data-direction=\"2\"]')?.getBoundingClientRect();return Boolean(l&&r&&r.left>l.left+20)})()")
                cdp.click("document.querySelector('#traffic-expand')")
                wait("document.querySelector('#traffic-expand').getAttribute('aria-pressed')==='false'")
                wait(f"Math.abs(document.querySelector('#traffic-sessions').getBoundingClientRect().width-{resized_width})<2")
                self.assertAlmostEqual(cdp.evaluate("document.querySelector('#traffic-sessions').getBoundingClientRect().width"),resized_width,delta=2)
                if os.environ.get('HYDRA_SCREENSHOT'):
                    shot=cdp.call('Page.captureScreenshot',{'format':'png'})
                    pathlib.Path(os.environ['HYDRA_SCREENSHOT']).write_bytes(base64.b64decode(shot['data']))
                cdp.call('Emulation.setDeviceMetricsOverride',{'width':390,'height':844,'deviceScaleFactor':1,'mobile':False})
                wait("!document.querySelector('[aria-label=\"Resize streams list\"]') && Boolean(document.querySelector('#traffic-stream [data-direction=\"2\"]'))")
                self.assertTrue(cdp.evaluate("document.documentElement.scrollWidth<=innerWidth"))
                self.assertTrue(cdp.evaluate("(()=>{const l=document.querySelector('#traffic-sessions').getBoundingClientRect(),r=document.querySelector('#traffic-stream').getBoundingClientRect();return r.top>=l.bottom-1})()"))
                cdp.click("document.querySelector('#traffic-expand')")
                wait("document.querySelector('#traffic-workspace').getBoundingClientRect().height===innerHeight")
                self.assertTrue(cdp.evaluate("document.querySelector('#traffic-expand').getBoundingClientRect().right<=innerWidth"))
                cdp.call('Input.dispatchKeyEvent',{'type':'keyDown','key':'Escape','code':'Escape','windowsVirtualKeyCode':27})
                cdp.call('Input.dispatchKeyEvent',{'type':'keyUp','key':'Escape','code':'Escape','windowsVirtualKeyCode':27})
                wait("document.querySelector('#traffic-expand').getAttribute('aria-pressed')==='false'")
                cdp.call('Emulation.setDeviceMetricsOverride',{'width':1280,'height':900,'deviceScaleFactor':1,'mobile':False})
                wait("Boolean(document.querySelector('[aria-label=\"Resize streams list\"]') && document.querySelector('#traffic-stream [data-direction=\"2\"]'))")
                service_id=cdp.evaluate("document.querySelector('#traffic-sessions [data-service-id]').dataset.serviceId")
                cdp.click("document.querySelector('#traffic-service')")
                wait("document.querySelectorAll('[role=listbox][data-state=open] [role=option]').length===3")
                cdp.click(f"[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()!=='All services'&&e.textContent.trim()!=={json.dumps(service['name'])})")
                wait("document.querySelector('#traffic-sessions').innerText.includes('No sessions for this service yet.')")
                self.assertFalse(cdp.evaluate("document.querySelector('#traffic-stream').innerText.includes('/browser-stream')"))
                cdp.click("document.querySelector('#traffic-service')")
                wait("document.querySelectorAll('[role=listbox][data-state=open] [role=option]').length===3")
                cdp.click(f"[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==={json.dumps(service['name'])})")
                wait("document.querySelector('#traffic-stream').innerText.includes('/browser-stream')")
                # Open the exact shared stream viewer beside an unsaved filter,
                # switch between two selected real flows, and retain form edits.
                threading.Thread(target=reply,daemon=True).start()
                with socket.create_connection(('127.0.0.1',int(service['listen_address'].split(':')[1])),timeout=3) as second:
                    second.sendall(b'GET /browser-second HTTP/1.1\r\nHost: browser-fixture\r\nConnection: close\r\n\r\n')
                    self.assertIn(b'200 OK',second.recv(4096))
                wait("document.querySelectorAll('#traffic-sessions [data-flow-id]').length===2")
                cdp.evaluate("document.querySelectorAll('#traffic-sessions [aria-label^=\"Select stream for filter\"]').forEach(e=>e.click())")
                wait("[...document.querySelectorAll('#page button')].some(b=>b.textContent.includes('Create filter from 2 streams'))")
                cdp.evaluate("[...document.querySelectorAll('#page button')].find(b=>b.textContent.includes('Create filter from 2 streams')).click()")
                wait("document.querySelectorAll('#filter-stream-workspace [data-preview-flow-id]').length===2")
                self.assertTrue(cdp.evaluate("(()=>{const l=document.querySelector('#filter-editor-form').getBoundingClientRect(),r=document.querySelector('#filter-stream-workspace').getBoundingClientRect();return r.left>=l.right})()"))
                selected_ids=cdp.evaluate("[...document.querySelectorAll('[data-preview-flow-id]')].map(e=>e.dataset.previewFlowId)")
                cdp.evaluate("{const e=document.querySelector('#filter-name');e.value='Keep my rules';e.dispatchEvent(new Event('input',{bubbles:true}));}")
                for selected_id in selected_ids:
                    cdp.click(f"document.querySelector('[data-preview-flow-id=\"{selected_id}\"]')")
                    wait(f"Boolean(document.querySelector('#filter-stream-workspace #traffic-stream [title=\"{selected_id}\"]'))")
                    wait("Boolean(document.querySelector('#filter-stream-workspace #traffic-stream [data-direction=\"2\"]'))")
                    self.assertIn('X-Reply',cdp.evaluate("document.querySelector('#filter-stream-workspace #traffic-stream').innerText"))
                    self.assertEqual(cdp.evaluate("document.querySelector('#filter-name').value"),'Keep my rules')
                cdp.evaluate("[...document.querySelectorAll('[role=dialog] button')].find(b=>b.textContent==='Cancel').click()")
                wait("!document.querySelector('[role=dialog]')")
                # Seed synthetic diagnostic history only; no worker/checker execution.
                output=b'A'*70000+b'OUTPUT_AFTER_64K FLAG002000'
                with sqlite3.connect(tmp/'db') as database:
                    now=int(time.time())
                    database.execute("INSERT INTO farm_services(id,name,port,created_at,updated_at,author_id) VALUES('browser-service','Readable service',12345,?,?,'alice')",[now,now])
                    database.execute("INSERT INTO farm_teams(id,name,host,created_at,updated_at,author_id) VALUES('local','Readable team','127.0.0.10',?,?,'alice')",[now,now])
                    database.execute("INSERT INTO farm_exploits(id,name,version,manifest,target_ids,initiated_by,created_at,updated_at) VALUES('browser-fixture','Readable exploit','v1','{}','[]','alice',?,?)",[now,now])
                    database.execute("INSERT INTO farm_runs(id,exploit_id,version,manifest,target_ids,initiated_by,status,created_at,logs_complete,stdout_bytes) VALUES('browser-run','browser-fixture','v1','{}','[\"local\"]','alice','succeeded',?,1,?)",[int(time.time()),len(output)])
                    snapshot=[{'id':'local','name':'Readable team / Readable service','host':'127.0.0.9','port':12345,'transport':'tcp','enabled':True,'revision':1}]
                    database.execute("UPDATE farm_runs SET service_id='browser-service',team_ids='[\"local\"]',target_snapshot=?,started_at=?,finished_at=? WHERE id='browser-run'",[json.dumps(snapshot),now-25,now-2])
                    database.execute("INSERT INTO farm_process_results(run_id,target_id,scope,status,started_at,finished_at,exit_code) VALUES('browser-run','local','per_target','succeeded',?,?,0)",[(now-25)*1000,(now-2)*1000])
                    for position in range(0,len(output),32768):
                        chunk=output[position:position+32768]
                        database.execute("INSERT INTO farm_log_chunks(run_id,stream,offset,raw_bytes,gzip) VALUES('browser-run','stdout',?,?,?)",[position,len(chunk),gzip.compress(chunk)])
                    for digest,is_new in [('new',1),('known',0)]:database.execute("INSERT INTO farm_flag_observations VALUES('browser-run','local',?,?)",[digest,is_new])
                    database.execute("INSERT INTO flag_submitter_batches(id,revision,flag_count,status,started_at,input_gzip,stdout_gzip,stderr_gzip) VALUES('11111111-1111-4111-8111-111111111111',0,1,'accepted',?,?,?,?)",[int(time.time()),gzip.compress(b'["FLAG002000"]'),gzip.compress(b'submission stdout FLAG002000'),gzip.compress(b'submission stderr FLAG002000')])
                for section in ['Filters','Traffic','Protocols','Farm','Flags','Settings','Users / audit']:
                    if section == 'Flags':
                        cdp.evaluate("window.__submitterViewRead=Promise.resolve(null);const fetchBeforeSubmitterCapture=window.fetch;window.fetch=async(...args)=>{const response=await fetchBeforeSubmitterCapture(...args);if(String(args[0]).endsWith('/api/v1/submitter')&&(!args[1]?.method||args[1].method==='GET'))window.__submitterViewRead=response.clone().json();return response}")
                    cdp.evaluate(f"[...document.querySelectorAll('nav button')].find(b=>b.textContent==={json.dumps(section)}).click()")
                    expected = {'Users / audit':'Users and audit','Farm':'Exploit farm','Flags':'Flag Manager'}.get(section,section)
                    wait(f"document.querySelector('#page h1')?.textContent==={json.dumps(expected)}")
                    wait("!document.querySelector('#page').innerText.includes('Loading')")
                    self.assertFalse(cdp.evaluate("document.querySelector('#error').textContent"))
                    self.assertFalse(cdp.evaluate("[...document.querySelectorAll('#page [role=alert]')].some(e=>e.textContent.trim())"),cdp.evaluate("document.querySelector('#page').innerText"))
                    if section == 'Settings':
                        wait("Boolean(document.querySelector('#settings-form button[type=submit]'))")
                        self.assertEqual(cdp.evaluate("document.querySelector('#settings-history-limit').value"),'128')
                        cdp.evaluate("{const e=document.querySelector('#settings-history-limit');e.value='2';e.dispatchEvent(new Event('input',{bubbles:true}));}")
                        cdp.evaluate("document.querySelector('#settings-form').requestSubmit()")
                        wait("document.querySelector('#page').innerText.includes('Saved to the configuration file.')")
                        self.assertEqual(json.loads((tmp/'operations.json').read_text())['traffic_history_per_service_mib'],2)
                        wait("document.querySelector('#page').innerText.includes('2 MiB') && !document.querySelector('#page').innerText.includes('pending Proxy')",seconds=10)
                        self.assertFalse(cdp.evaluate("document.querySelector('#page').innerText.includes('pending a Core restart')"))
                        status=json.loads(subprocess.check_output([str(BIN/'hydra-core'),'--socket',sock,'status'],text=True))
                        self.assertEqual(status['history_per_service_bytes'],2*1024*1024)

                        cdp.evaluate("const input=document.querySelector('#settings-pattern');input.value='FLAG[0-9]{6}|EXTRA[0-9]{6}';input.dispatchEvent(new Event('input',{bubbles:true}));")
                        cdp.evaluate("document.querySelector('#settings-form').requestSubmit()")
                        wait("document.querySelector('#page').innerText.includes('Saved to the configuration file.')")
                        self.assertEqual(json.loads((tmp/'operations.json').read_text())['flag_pattern'],'FLAG[0-9]{6}|EXTRA[0-9]{6}')
                        self.assertEqual((tmp/'operations.json').stat().st_mode & 0o777,0o600)
                        self.assertIn('pending a Core restart',cdp.evaluate("document.querySelector('#page').innerText"))
                    if section == 'Users / audit':
                        wait("Boolean(document.querySelector('#create-user-form'))")
                        cdp.evaluate("for(const [id,value] of [['create-user-name','helper'],['create-user-password','helper-password-1234']]){const input=document.getElementById(id);input.value=value;input.dispatchEvent(new Event('input',{bubbles:true}));}")
                        cdp.evaluate("document.querySelector('#create-user-form').requestSubmit()")
                        wait("document.querySelector('#create-user-form').innerText.includes('Created helper.')")
                        wait("[...document.querySelectorAll('td')].some(e=>e.textContent==='helper')")
                        self.assertFalse(cdp.evaluate("[...document.querySelectorAll('td')].some(e=>/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(e.textContent)&&e.cellIndex===0)"),'Actor/user references must show names')
                    if section == 'Filters':
                        # Manage a real Python checker through the filter page.
                        # Enforcement itself is exercised in request_checkers.py.
                        cdp.click("[...document.querySelectorAll('#page button')].find(b=>b.textContent.includes('Request checkers'))")
                        wait("[...document.querySelectorAll('#page button')].some(b=>b.textContent.trim()==='Add Python checker' && !b.disabled)")
                        cdp.click("[...document.querySelectorAll('#page button')].find(b=>b.textContent.trim()==='Add Python checker')")
                        wait("Boolean(document.querySelector('#checker-id-input'))")
                        wait("Boolean(document.querySelector('#checker-source-input[role=textbox][aria-multiline=true]'))")
                        self.assertEqual(cdp.evaluate("document.querySelector('#checker-source-input').getAttribute('aria-label')"),'New Python checker source')

                        # Check that the creation controls remain reachable on
                        # a narrow screen, then return to the desktop layout.
                        cdp.call('Emulation.setDeviceMetricsOverride',{'width':390,'height':844,'deviceScaleFactor':1,'mobile':False})
                        reachable = cdp.evaluate("(()=>{const selectors=['#checker-id-input','#checker-source-input[role=textbox]'];const button=[...document.querySelectorAll('#page button')].find(e=>e.textContent.trim()==='Create paused checker');for(const selector of selectors){const e=document.querySelector(selector);const visual=e?.closest('.cm-editor')||e;visual.scrollIntoView({block:'center',behavior:'instant'});const r=visual.getBoundingClientRect();if(r.width===0||r.left<0||r.right>innerWidth||r.top<0||r.bottom>innerHeight)return false;}button.scrollIntoView({block:'center',behavior:'instant'});const r=button.getBoundingClientRect();return r.width>0&&r.left>=0&&r.right<=innerWidth&&r.top>=0&&r.bottom<=innerHeight&&document.documentElement.scrollWidth<=innerWidth&&document.body.scrollWidth<=innerWidth})()")
                        reachability_debug = cdp.evaluate("(()=>{const fields=['#checker-id-input','#checker-source-input[role=textbox]'].map(selector=>{const e=document.querySelector(selector);const r=e?.getBoundingClientRect();const surface=e?.closest('.cm-editor');const s=surface?.getBoundingClientRect();return {selector,rect:r&&{x:r.x,y:r.y,width:r.width,height:r.height,right:r.right,bottom:r.bottom},surface:s&&{x:s.x,y:s.y,width:s.width,height:s.height,right:s.right,bottom:s.bottom},scroll:e?.scrollHeight,client:e?.clientHeight}});const button=[...document.querySelectorAll('#page button')].find(e=>e.textContent.trim()==='Create paused checker');return JSON.stringify({viewport:innerWidth,pageWidth:document.querySelector('#page').scrollWidth,fields,button:button?.getBoundingClientRect()})})()")
                        self.assertTrue(reachable, 'Checker fields and create button must remain reachable at mobile width: '+reachability_debug)
                        cdp.call('Emulation.setDeviceMetricsOverride',{'width':1280,'height':900,'deviceScaleFactor':1,'mobile':False})
                        wait("innerWidth===1280 && innerHeight===900")
                        cdp.evaluate("{const e=document.querySelector('#checker-id-input');e.value='browser-managed';e.dispatchEvent(new Event('input',{bubbles:true}));}")
                        upload_source = '# UTF-8 marker: Ω 🚩\r\n\r\ndef check(request: bytes) -> bool:\r\n    return True\r\n'
                        uploaded_script = tmp / 'browser-managed.py'
                        uploaded_script.write_bytes(upload_source.encode('utf-8'))
                        document = cdp.call('DOM.getDocument')['root']['nodeId']
                        source_file_node = cdp.call('DOM.querySelector',{'nodeId':document,'selector':'#page input[type="file"]'})['nodeId']
                        cdp.call('DOM.setFileInputFiles',{'nodeId':source_file_node,'files':[str(uploaded_script)]})
                        wait("(()=>{const editor=document.querySelector('#checker-source-input');const surface=editor?.closest('.cm-editor');return editor?.getAttribute('role')==='textbox' && editor.innerText.includes('UTF-8 marker: Ω 🚩') && surface?.querySelector('.cm-gutters .cm-lineNumbers') && getComputedStyle(surface).backgroundColor==='rgb(0, 0, 0)' && [...editor.querySelectorAll('.cm-line span')].some(token=>getComputedStyle(token).color!==getComputedStyle(editor).color)})()")
                        (artifacts / 'hydra-python-checker-desktop.png').write_bytes(base64.b64decode(cdp.call('Page.captureScreenshot',{'format':'png'})['data']))
                        cdp.evaluate("window.__checkerProgramsRead=Promise.resolve(null);const fetchBeforeCheckerCapture=window.fetch;window.fetch=async(...args)=>{const response=await fetchBeforeCheckerCapture(...args);if(String(args[0]).endsWith('/api/v1/filters/request-checkers/programs')&&(!args[1]?.method||args[1].method==='GET'))window.__checkerProgramsRead=response.clone().json();return response}")
                        cdp.click("[...document.querySelectorAll('#page button')].find(b=>b.textContent.trim()==='Create paused checker')")
                        wait("document.querySelector('#page').innerText.includes('browser-managed') && [...document.querySelectorAll('#page button')].some(b=>b.textContent.trim()==='Edit source')")
                        wait(f"window.__checkerProgramsRead?.then(body=>body.programs.find(program=>program.id==='browser-managed')?.source==={json.dumps(upload_source)})")
                        cdp.click("[...document.querySelectorAll('#page button')].find(b=>b.textContent.trim()==='Edit source')")
                        wait("Boolean(document.querySelector('#checker-source-editor[role=textbox][aria-multiline=true]')) && [...document.querySelectorAll('#page button')].some(b=>b.textContent.trim()==='Save source')")
                        self.assertEqual(cdp.evaluate("document.querySelector('#checker-source-editor').getAttribute('aria-label')"),'Python checker source')
                        edited_source = '# UTF-8 marker: π 🔒\r\n\r\ndef check(request: bytes) -> bool:\r\n    return False\r\n'
                        replace_editor('#checker-source-editor',edited_source)
                        wait("document.querySelector('#checker-source-editor')?.innerText.includes('UTF-8 marker: π 🔒') && document.querySelector('#checker-source-editor')?.innerText.includes('return False')")
                        cdp.call('Input.dispatchKeyEvent',{'type':'keyDown','key':'Control','code':'ControlLeft','modifiers':2})
                        cdp.call('Input.dispatchKeyEvent',{'type':'keyDown','key':'z','code':'KeyZ','modifiers':2})
                        cdp.call('Input.dispatchKeyEvent',{'type':'keyUp','key':'z','code':'KeyZ','modifiers':2})
                        cdp.call('Input.dispatchKeyEvent',{'type':'keyUp','key':'Control','code':'ControlLeft'})
                        wait("document.querySelector('#checker-source-editor')?.innerText.includes('UTF-8 marker: Ω 🚩') && document.querySelector('#checker-source-editor')?.innerText.includes('return True')")
                        cdp.call('Input.dispatchKeyEvent',{'type':'keyDown','key':'Control','code':'ControlLeft','modifiers':2})
                        cdp.call('Input.dispatchKeyEvent',{'type':'keyDown','key':'y','code':'KeyY','modifiers':2})
                        cdp.call('Input.dispatchKeyEvent',{'type':'keyUp','key':'y','code':'KeyY','modifiers':2})
                        cdp.call('Input.dispatchKeyEvent',{'type':'keyUp','key':'Control','code':'ControlLeft'})
                        wait("document.querySelector('#checker-source-editor')?.innerText.includes('UTF-8 marker: π 🔒') && document.querySelector('#checker-source-editor')?.innerText.includes('return False')")
                        cdp.click("document.querySelector('#checker-source-editor')")
                        for _ in range(2):
                            for kind in ['keyDown','keyUp']:cdp.call('Input.dispatchKeyEvent',{'type':kind,'key':'Escape','code':'Escape','windowsVirtualKeyCode':27})
                        for kind in ['keyDown','keyUp']:cdp.call('Input.dispatchKeyEvent',{'type':kind,'key':'Tab','code':'Tab','windowsVirtualKeyCode':9})
                        wait("document.activeElement!==document.querySelector('#checker-source-editor')")
                        cdp.click("[...document.querySelectorAll('#page button')].find(b=>b.textContent.trim()==='Save source')")
                        wait("[...document.querySelectorAll('#page button')].some(b=>b.textContent.trim()==='Edit source')")
                        wait(f"window.__checkerProgramsRead?.then(body=>body.programs.find(program=>program.id==='browser-managed')?.source==={json.dumps(edited_source)})")
                        cdp.evaluate("window.confirm=()=>true")
                        cdp.click("[...document.querySelectorAll('#page button')].find(b=>b.textContent.trim()==='Delete checker')")
                        wait("document.querySelector('#page').innerText.includes('No checker programs are configured.')")
                        cdp.evaluate("[...document.querySelectorAll('#page button')].find(b=>b.textContent==='Add filter').click()")
                        wait("Boolean(document.querySelector('[role=dialog]')?.innerText.includes('Pattern'))")
                        self.assertFalse(cdp.evaluate("Boolean(document.querySelector('[role=dialog] textarea:not(#filter-description)'))"), 'Filter editor must not request an expression JSON textarea')
                        cdp.click("[...document.querySelectorAll('[role=dialog] button')].find(b=>b.textContent==='Advanced expression')")
                        wait("Boolean(document.querySelector('[role=dialog]')?.innerText.includes('Match conditions'))")
                        self.assertTrue(cdp.evaluate("[...document.querySelectorAll('[role=dialog] button')].some(b=>b.textContent==='Add group')"))
                        cdp.evaluate("[...document.querySelectorAll('[role=dialog] button')].find(b=>b.textContent==='Add group').click()")
                        wait("document.querySelectorAll('[role=dialog] [aria-label=\"Group match mode\"]').length===2")
                        # Remove the root condition, then its only remaining group.
                        # This must edit the expression without submitting the form.
                        cdp.evaluate("document.querySelector('[role=dialog] [aria-label=\"Remove rule\"]').click()")
                        wait("document.querySelectorAll('[role=dialog] [aria-label=\"Remove rule\"]').length===1")
                        cdp.evaluate("document.querySelector('[role=dialog] [aria-label=\"Remove group\"]').click()")
                        wait("document.querySelectorAll('[role=dialog] [aria-label=\"Group match mode\"]').length===1")
                        self.assertEqual(cdp.evaluate("document.querySelectorAll('[role=dialog] [aria-label=\"Remove rule\"]').length"), 1)
                        self.assertFalse(cdp.evaluate("Boolean(document.querySelector('[role=dialog] [aria-label=\"Remove group\"]'))"))
                        cdp.evaluate("[...document.querySelectorAll('[role=dialog] button')].find(b=>b.textContent==='Cancel').click()")
                        wait("!document.querySelector('[role=dialog]')")

                        # Create a filter checker through the new script tab and
                        # verify its per-service switch can be disabled and re-enabled.
                        cdp.evaluate("[...document.querySelectorAll('#page button')].find(b=>b.textContent==='Add filter').click()")
                        wait("Boolean(document.querySelector('[role=dialog]'))")
                        cdp.click("[...document.querySelectorAll('[role=dialog] button[role=tab]')].find(b=>b.textContent==='Script')")
                        wait("Boolean(document.querySelector('#script-name') && document.querySelector('#script-source[role=textbox][aria-multiline=true]'))")
                        cdp.evaluate("{const e=document.querySelector('#script-name');e.value='browser-script';e.dispatchEvent(new Event('input',{bubbles:true}));}")
                        replace_editor('#script-source', 'def check(request: bytes) -> bool:\n    return True\n')
                        cdp.click("[...document.querySelectorAll('[role=dialog] button')].find(b=>b.textContent==='Save script')")
                        wait("[...document.querySelectorAll('#page code')].some(e=>e.textContent==='browser-script')")
                        cdp.click("[...document.querySelectorAll('#page aside[aria-label=Services] button')].find(b=>b.textContent.includes('Unused service'))")
                        wait("!document.querySelector('[aria-label=\"Enable browser-script\"]')?.disabled")
                        cdp.click("document.querySelector('[aria-label=\"Enable browser-script\"]')")
                        wait("document.querySelector('[aria-label=\"Enable browser-script\"]')?.getAttribute('aria-checked')==='false'")
                        cdp.click("document.querySelector('[aria-label=\"Enable browser-script\"]')")
                        wait("document.querySelector('[aria-label=\"Enable browser-script\"]')?.getAttribute('aria-checked')==='true'")
                        cdp.click("[...document.querySelectorAll('#page aside[aria-label=Services] button')].find(b=>b.textContent.includes('All services'))")
                        # Default creation and editing enforce directly: no
                        # staging in Shadow and no test-message selection.
                        cdp.evaluate("[...document.querySelectorAll('#page button')].find(b=>b.textContent==='Add filter').click()")
                        wait("Boolean(document.querySelector('#filter-mode'))")
                        self.assertIn('Enforce', cdp.evaluate("document.querySelector('#filter-mode').textContent"))
                        cdp.click("document.querySelector('[aria-label=\"Filter service\"]')")
                        wait("document.querySelectorAll('[role=listbox][data-state=open] [role=option]').length===2")
                        cdp.click(f"[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==={json.dumps(service['name'])})")
                        cdp.click("[...document.querySelectorAll('[role=dialog] button')].find(b=>b.textContent==='Advanced expression')")
                        wait("Boolean(document.querySelector('[aria-label=\"Rule direction\"]'))")
                        cdp.evaluate("{const e=document.querySelector('#filter-name');e.value='Browser filter';e.dispatchEvent(new Event('input',{bubbles:true}));}")
                        self.assertIn('Both directions', cdp.evaluate("document.querySelector('[aria-label=\"Rule direction\"]').textContent"))
                        cdp.click("document.querySelector('[aria-label=\"Rule direction\"]')")
                        wait("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(e=>e.textContent.trim()==='Client → server')")
                        cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==='Client → server')")
                        cdp.click("document.querySelector('[aria-label=\"Filter action\"]')")
                        wait("document.querySelectorAll('[role=listbox][data-state=open] [role=option]').length>2")
                        cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==='Block HTTP request')")
                        cdp.click("document.querySelector('[aria-label=\"Rule field\"]')")
                        wait("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(e=>e.textContent.trim()==='HTTP method')")
                        cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==='HTTP method')")
                        wait("document.querySelector('[aria-label=\"Rule field\"]')?.innerText.trim()==='HTTP method'")
                        cdp.evaluate("{const e=document.querySelector('[aria-label=\"Rule text value\"]');e.value='GET';e.dispatchEvent(new Event('input',{bubbles:true}));}")
                        cdp.click("[...document.querySelectorAll('[role=dialog] button')].find(b=>b.textContent==='Add condition')")
                        wait("document.querySelectorAll('[aria-label=\"Rule direction\"]').length===2")
                        cdp.click("document.querySelectorAll('[aria-label=\"Rule direction\"]')[1]")
                        wait("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(e=>e.textContent.trim()==='Server → client')")
                        cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==='Server → client')")
                        cdp.click("document.querySelectorAll('[aria-label=\"Rule field\"]')[1]")
                        wait("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(e=>e.textContent.trim()==='HTTP method')")
                        cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==='HTTP method')")
                        wait("document.querySelectorAll('[aria-label=\"Rule field\"]')[1]?.innerText.trim()==='HTTP method'")
                        wait("document.querySelectorAll('[aria-label=\"Rule text value\"]').length===2")
                        cdp.evaluate("{const e=document.querySelectorAll('[aria-label=\"Rule text value\"]')[1];e.value='POST';e.dispatchEvent(new Event('input',{bubbles:true}));}")
                        cdp.click("document.querySelector('[aria-label=\"Group match mode\"]')")
                        wait("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(e=>e.textContent.trim()==='any (OR)')")
                        cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==='any (OR)')")
                        cdp.evaluate("document.querySelector('[role=dialog] form').requestSubmit()")
                        wait("!document.querySelector('[role=dialog]') && Boolean(document.querySelector('[data-filter-card][aria-label=\"Browser filter\"]'))")
                        self.assertEqual(cdp.evaluate("[...document.querySelectorAll('[data-filter-card] dd')].filter(e=>e.textContent==='alice').length"),2,'Filter author/editor must resolve to username')
                        self.assertEqual(cdp.evaluate("document.querySelector('[data-filter-card]').style.borderLeftColor"),'rgb(102, 217, 232)')
                        wait("document.querySelector('[data-filter-application]').dataset.state==='enabled'")
                        cdp.click("document.querySelector('[data-filter-card] [role=switch]')")
                        wait("document.querySelector('[data-filter-desired]').dataset.enabled==='false' && document.querySelector('[data-filter-application]').dataset.state==='disabled'")
                        cdp.click("document.querySelector('[data-filter-card] [role=switch]')")
                        wait("document.querySelector('[data-filter-desired]').dataset.enabled==='true' && document.querySelector('[data-filter-application]').dataset.state==='enabled'")
                        cdp.evaluate("[...document.querySelectorAll('#page button')].find(b=>b.textContent==='Edit').click()")
                        wait("Boolean(document.querySelector('#filter-status'))")
                        self.assertIn('Client → server', cdp.evaluate("document.querySelector('[aria-label=\"Rule direction\"]').textContent"))
                        self.assertIn('Server → client',cdp.evaluate("document.querySelectorAll('[aria-label=\"Rule direction\"]')[1].textContent"))
                        cdp.evaluate("{ const e = document.querySelector('#filter-status'); e.value='402'; e.dispatchEvent(new Event('input',{bubbles:true})); }")
                        cdp.evaluate("document.querySelector('[role=dialog] form').requestSubmit()")
                        wait("!document.querySelector('[role=dialog]') && Boolean(document.querySelector('[data-filter-card][aria-label=\"Browser filter\"]'))")
                        # Saved-traffic testing is still an optional diagnostic.
                        wait("Boolean(document.querySelector('[data-filter-card] button') && [...document.querySelectorAll('[data-filter-card] button')].some(b=>b.textContent==='Test'&&!b.disabled))")
                        cdp.evaluate("[...document.querySelectorAll('[data-filter-card] button')].find(b=>b.textContent==='Test').click()")
                        wait("Boolean(document.querySelector('[role=dialog] [aria-label=\"Test message\"]'))")
                        cdp.evaluate("[...document.querySelectorAll('[role=dialog] button')].find(b=>b.textContent==='Test only').click()")
                        wait("document.querySelector('[role=dialog]').innerText.includes('Matched.')")
                        cdp.evaluate("[...document.querySelectorAll('[role=dialog] button')].find(b=>b.textContent==='Close').click()")
                        wait("!document.querySelector('[role=dialog]')")
                        self.assertFalse(cdp.evaluate("[...document.querySelectorAll('#page [role=alert]')].some(e=>e.textContent.trim())"))
                        # A real blocked HTTP flow is absent by default and
                        # appears only when the operator requests blocked traffic.
                        with socket.create_connection(('127.0.0.1',int(service['listen_address'].split(':')[1])),timeout=3) as blocked:
                            blocked.sendall(b'GET /blocked-browser HTTP/1.1\r\nHost: browser-fixture\r\nConnection: close\r\n\r\n')
                            self.assertIn(b'402',blocked.recv(4096))
                        cdp.evaluate("[...document.querySelectorAll('nav button')].find(b=>b.textContent==='Traffic').click()")
                        wait(TRAFFIC_LIVE_EXPRESSION + " && Boolean(document.querySelector('#traffic-sessions [data-flow-id]'))")
                        self.assertFalse(cdp.evaluate("Boolean(document.querySelector('#traffic-sessions [data-blocked=\"true\"]'))"))
                        cdp.click("document.querySelector('#traffic-show-blocked')")
                        wait("Boolean(document.querySelector('#traffic-sessions [data-blocked=\"true\"]'))")
                        cdp.click("document.querySelector('#traffic-show-blocked')")
                        wait("!document.querySelector('#traffic-sessions [data-blocked=\"true\"]')")
                        wait("Boolean(document.querySelector('#traffic-sessions [data-has-flag=\"true\"]'))")
                    if section == 'Farm':
                        wait("document.querySelector('[data-farm-effectiveness]')?.innerText.includes('Readable exploit')")
                        matrix=cdp.evaluate("document.querySelector('[data-farm-effectiveness]').innerText")
                        for text in ['Readable team','Readable service','1 new / 1 duplicate','1 recorded target attempts','succeeded','23.0 s']:
                            self.assertIn(text,matrix)
                        cdp.click("document.querySelector('[data-farm-effectiveness] button[aria-label=\"Logs for Readable exploit against Readable team\"]')")
                        wait("Boolean(document.querySelector('[data-log-stream=stdout]'))")
                        cdp.evaluate("[...document.querySelectorAll('[role=dialog] button')].find(b=>b.textContent==='Close logs').click()")
                        wait("!document.querySelector('[role=dialog]')")
                        self.assertFalse(cdp.evaluate("Boolean(document.querySelector('[data-farm-runs]'))"))
                        cdp.click("[...document.querySelectorAll('#page button[aria-expanded]')].find(e=>e.textContent.trim().startsWith('Run history'))")
                        wait("document.querySelector('[data-farm-runs]')?.innerText.includes('Readable exploit')")
                        self.assertEqual(cdp.evaluate("[...document.querySelectorAll('[data-farm-runs] th')].map(e=>e.textContent)"),['Run time','Team','Exploit','Service','New / duplicate','State','Actions'])
                        for text in ['Readable team','127.0.0.9','Readable service','23s']:
                            self.assertIn(text,cdp.evaluate("document.querySelector('[data-farm-runs]').innerText"))
                        wait("[...document.querySelectorAll('[data-farm-runs] button')].some(b=>b.textContent==='Logs'&&!b.disabled)")
                        cdp.click("[...document.querySelectorAll('[data-farm-runs] button')].find(b=>b.textContent==='Logs')")
                        wait("document.querySelector('[data-log-stream=stdout]')?.innerText.includes('OUTPUT_AFTER_64K FLAG002000')")
                        self.assertIn('1 new · 1 existing',cdp.evaluate("document.querySelector('[role=dialog]').innerText"))
                        cdp.evaluate("[...document.querySelectorAll('[data-log-stream=stdout] button')].find(b=>b.textContent==='First retained').click()")
                        wait("!document.querySelector('[data-log-stream=stdout] pre').innerText.includes('OUTPUT_AFTER_64K')")
                        cdp.evaluate("[...document.querySelectorAll('[data-log-stream=stdout] button')].find(b=>b.textContent==='Next output').click()")
                        wait("document.querySelector('[data-log-stream=stdout] pre').innerText.includes('OUTPUT_AFTER_64K')")
                        self.assertIn('Run logs',cdp.evaluate("document.querySelector('[role=dialog]').innerText"))
                        cdp.evaluate("[...document.querySelectorAll('[role=dialog] button')].find(b=>b.textContent==='Close logs').click()")
                        wait("!document.querySelector('[role=dialog]')")
                        wait("document.activeElement?.textContent==='Logs'")
                    if section == 'Protocols':
                        root=pathlib.Path(__file__).resolve().parents[2]
                        candidates=[pathlib.Path(os.environ['HYDRA_WASM_SAMPLE'])] if os.environ.get('HYDRA_WASM_SAMPLE') else [root/'plugins/examples/length-prefixed/target/wasm32-unknown-unknown/release/hydra_length_prefixed_example.wasm']
                        module=next((p for p in candidates if p.is_file()),None)
                        self.assertIsNotNone(module,'Build an example decoder or set HYDRA_WASM_SAMPLE before the browser smoke')
                        for field,value in [('protocol-id','browser-wasm'),('protocol-version','v1')]:
                            cdp.evaluate(f"{{const e=document.getElementById({json.dumps(field)});e.value={json.dumps(value)};e.dispatchEvent(new Event('input',{{bubbles:true}}));}}")
                        document=cdp.call('DOM.getDocument')['root']['nodeId']
                        file_node=cdp.call('DOM.querySelector',{'nodeId':document,'selector':'#protocol-file'})['nodeId']
                        cdp.call('DOM.setFileInputFiles',{'nodeId':file_node,'files':[str(module)]})
                        cdp.evaluate("document.querySelector('#protocol-file').closest('form').requestSubmit()")
                        wait("[...document.querySelectorAll('#page td')].some(e=>e.textContent==='installed')")
                        cdp.evaluate("[...document.querySelectorAll('nav button')].find(b=>b.textContent==='Services').click()")
                        wait("document.querySelector('#page h1')?.textContent==='Services' && !document.querySelector('#page').innerText.includes('Loading')")
                        def edit_other():
                            cdp.evaluate(f"[...[...document.querySelectorAll('#page tr')].find(e=>e.innerText.includes({json.dumps(other['listen_address'])})).querySelectorAll('button')].find(b=>b.textContent==='Edit').click()")
                            wait("document.querySelector('#service-profile') && [...document.querySelectorAll('#service-protocol-options option')].some(e=>e.value==='custom:browser-wasm/v1')")
                        edit_other()
                        cdp.evaluate("{const e=document.querySelector('#service-profile');e.value='custom:browser-wasm/v1';e.dispatchEvent(new Event('input',{bubbles:true}));}")
                        cdp.evaluate("document.querySelector('#service-form').requestSubmit()")
                        wait("!document.querySelector('#service-form') && document.querySelector('#page').innerText.includes('browser-wasm / v1 · WASM')")
                        with sqlite3.connect(tmp/'db') as db:self.assertEqual(db.execute("SELECT protocol_id,version,applied FROM protocol_activations WHERE protocol_id='browser-wasm'").fetchall(),[('browser-wasm','v1',1)])
                        edit_other()
                        self.assertEqual(cdp.evaluate("document.querySelector('#service-profile').value"),'custom:browser-wasm/v1')
                        cdp.evaluate("{const e=document.querySelector('#service-profile');e.value='utf8';e.dispatchEvent(new Event('input',{bubbles:true}));}")
                        cdp.evaluate("document.querySelector('#service-form').requestSubmit()")
                        wait("!document.querySelector('#service-form') && document.querySelector('#page').innerText.includes('UTF-8 text')")
                        with sqlite3.connect(tmp/'db') as db:self.assertEqual(db.execute("SELECT count(*) FROM protocol_activations WHERE protocol_id='browser-wasm'").fetchone()[0],0)
                        wait("[...document.querySelectorAll('#page button')].some(b=>b.textContent==='Add service' && !b.disabled)")
                        cdp.evaluate("[...document.querySelectorAll('#page button')].find(b=>b.textContent==='Add service').click()")
                        wait("document.querySelector('#service-form') && !document.querySelector('#service-save').disabled")
                        # TCP and UDP can share a listener address. Lose the activation
                        # response after Proxy applies it, then retry with a built-in.
                        cdp.click("document.querySelector('#service-transport')")
                        wait("document.querySelector('[role=option][data-state]') !== null")
                        cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==='UDP')")
                        cdp.evaluate("{const original=window.fetch;window.fetch=async(...args)=>{const response=await original(...args);if(String(args[0]).endsWith('/protocols/browser-wasm/v1/activate')){window.fetch=original;throw new Error('Injected response loss after real activation');}return response;};}")
                        for field,value in [('service-name','Browser UDP activation'),('service-listener',other['listen_address']),('service-backend',other['upstream_address']),('service-profile','custom:browser-wasm/v1')]:
                            cdp.evaluate(f"{{const e=document.getElementById({json.dumps(field)});e.value={json.dumps(value)};e.dispatchEvent(new Event('input',{{bubbles:true}}));}}")
                        cdp.evaluate("document.querySelector('#service-form').requestSubmit()")
                        wait("document.querySelector('#service-form').innerText.includes('Service saved; decoder was not confirmed.')")
                        with sqlite3.connect(tmp/'db') as db:
                            active_id=db.execute("SELECT service_id FROM protocol_activations WHERE protocol_id='browser-wasm'").fetchone()[0]
                            snapshot=json.loads(db.execute('SELECT services FROM control_state WHERE id=1').fetchone()[0])
                            active_service=next(s for s in snapshot if s['id']==active_id)
                            self.assertEqual(active_service['transport'],'udp')
                            self.assertEqual(active_service['listen_address'],other['listen_address'])
                        cdp.evaluate("{const e=document.querySelector('#service-profile');e.value='utf8';e.dispatchEvent(new Event('input',{bubbles:true}));}")
                        cdp.evaluate("document.querySelector('#service-form').requestSubmit()")
                        wait("!document.querySelector('#service-form')")
                        with sqlite3.connect(tmp/'db') as db:self.assertEqual(db.execute("SELECT count(*) FROM protocol_activations WHERE protocol_id='browser-wasm'").fetchone()[0],0)
                        wait("[...document.querySelectorAll('#page button')].some(b=>b.textContent==='Add service' && !b.disabled)")
                        cdp.evaluate("[...document.querySelectorAll('#page button')].find(b=>b.textContent==='Add service').click()")
                        wait("document.querySelector('#service-form') && !document.querySelector('#service-save').disabled")
                        # Exercise actual module output through both processes and the
                        # default conversation view, not just the activation ACK.
                        with socket.socket() as decoder_backend:
                            decoder_backend.bind(('127.0.0.1',0));decoder_backend.listen();decoder_backend.settimeout(10)
                            marker=b'WASM_DECODED_RESPONSE_'+b'K'*1230
                            if 'crypter' in str(module):
                                def h2frame(kind,flags,stream,payload):return len(payload).to_bytes(3,'big')+bytes([kind,flags])+stream.to_bytes(4,'big')+payload
                                path=b'/crypter.Crypter/GetUserPublicKey'
                                headers=b'\x83\x86\x04'+bytes([len(path)])+path
                                request_bytes=b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n'+h2frame(4,0,0,b'')+h2frame(1,4,1,headers)+h2frame(0,1,1,b'\x00\x00\x00\x00\x00')
                                n=len(marker);protobuf=b'\x0a'+bytes([(n&127)|128,n>>7])+marker
                                grpc=b'\x00'+len(protobuf).to_bytes(4,'big')+protobuf
                                response_bytes=h2frame(4,0,0,b'')+h2frame(1,4,1,b'\x88')+h2frame(0,1,1,grpc)
                                expected_field_key='crypter.GetUserPublicKeyResponse.n'
                                expected_field_type=None
                                expected_payload=None
                            else:
                                request_bytes=b'\x00\x04PING'
                                response_bytes=len(marker).to_bytes(2,'big')+marker
                                # The decoder strips the u16 length prefix, so the
                                # decoded bytes field is exactly the marker payload.
                                expected_field_key='protocol.payload'
                                expected_field_type='bytes'
                                expected_payload=list(response_bytes[2:])
                            def decoder_reply():
                                try:
                                    peer,_=decoder_backend.accept()
                                    with peer:
                                        peer.settimeout(3);received=b''
                                        while len(received)<len(request_bytes):
                                            data=peer.recv(4096)
                                            if not data:return
                                            received+=data
                                        peer.sendall(response_bytes)
                                except OSError:pass
                            threading.Thread(target=decoder_reply,daemon=True).start()
                            new_module_service={**service,'name':'Browser decoder','listen_address':f'127.0.0.1:{port()}','upstream_address':f'127.0.0.1:{decoder_backend.getsockname()[1]}','protocol_id':'custom:browser-wasm/v1'}
                            fill_service(new_module_service)
                            with sqlite3.connect(tmp/'db') as db:
                                activation=db.execute("SELECT service_id,protocol_id,version,applied FROM protocol_activations WHERE protocol_id='browser-wasm'").fetchone()
                                self.assertEqual(activation[1:],('browser-wasm','v1',1));module_service_id=activation[0]
                            cdp.evaluate("[...document.querySelectorAll('nav button')].find(b=>b.textContent==='Traffic').click()")
                            wait(TRAFFIC_LIVE_EXPRESSION)
                            cdp.click("document.querySelector('#traffic-service')")
                            wait("document.querySelectorAll('[role=listbox][data-state=open] [role=option]').length>2")
                            cdp.click(f"[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==={json.dumps(new_module_service['name'])})")
                            with socket.create_connection(('127.0.0.1',int(new_module_service['listen_address'].split(':')[1])),timeout=3) as peer:
                                peer.sendall(request_bytes);received=b''
                                while len(received)<len(response_bytes):received+=peer.recv(4096)
                                self.assertEqual(received,response_bytes)
                            # Core's display summary deliberately omits binary fields,
                            # so the server frame's visible text is only its protocol
                            # and message type. The decoded bytes are still part of the
                            # contract: read them from the Inspect fields table.
                            wait("Boolean(document.querySelector('#traffic-stream [data-direction=\"2\"]'))")
                            self.assertEqual(cdp.evaluate("document.querySelector('[aria-label=\"Stream view\"]').textContent"),'Decoded conversation')
                            server_card="[...document.querySelectorAll('#traffic-stream [data-direction=\"2\"]')].at(-1)"
                            if expected_payload is None:
                                self.assertIn('WASM_DECODED_RESPONSE_',cdp.evaluate(f"{server_card}.innerText"))
                            else:
                                server_summary=cdp.evaluate(f"{server_card}.innerText")
                                self.assertIn('length-prefixed',server_summary)
                                self.assertIn('frame',server_summary)
                            cdp.evaluate(f"[...({server_card}).querySelectorAll('button')].find(b=>b.textContent==='Inspect fields').click()")
                            wait(f"Boolean(({server_card}).querySelector('table tbody tr'))")
                            inspected=cdp.evaluate(f"[...({server_card}).querySelectorAll('table tbody tr')].map(r=>[...r.cells].map(c=>c.textContent))")
                            field_row=next((row for row in inspected if row[0]==expected_field_key),['','',''])
                            self.assertNotEqual(field_row[0],'',f'{expected_field_key} is missing from the Inspect fields table: {inspected}')
                            if expected_field_type is not None:self.assertEqual(field_row[1],expected_field_type)
                            decoded_payload=json.loads(field_row[2])
                            if expected_payload is None:self.assertIn('WASM_DECODED_RESPONSE_',decoded_payload)
                            else:self.assertEqual(decoded_payload,expected_payload)
                            cli_credentials=tmp/'deploy-credentials.json'
                            cli_credentials.write_text(json.dumps({'username':'alice','password':'test-password-1234'}));cli_credentials.chmod(0o600)
                            result=subprocess.run(['python3',str(root/'scripts/deploy-protocol-remote.py'),str(module),'--id','browser-wasm','--version','script-v2','--service',module_service_id,'--origin',f'https://localhost:{ap}','--ca',str(tmp/'tls.crt'),'--credentials',str(cli_credentials)],check=True,capture_output=True,text=True,timeout=30)
                            self.assertTrue(json.loads(result.stdout)['applied'])
                            with sqlite3.connect(tmp/'db') as db:self.assertEqual(db.execute('SELECT version,applied FROM protocol_activations WHERE service_id=?',[module_service_id]).fetchone(),('script-v2',1))

                    if section == 'Flags':
                        wait("Boolean(document.querySelector('#submitter-forcad-ip'))")
                        self.assertTrue(cdp.evaluate("document.querySelector('#page').innerText.includes('ForcAD')"))
                        wait("window.__submitterViewRead?.then(view=>view.config.mode==='forcad' && !view.config.enabled && !view.config.team_token_configured)")
                        for field,value in [('submitter-forcad-ip','127.0.0.1'),('submitter-forcad-port','8080'),('submitter-forcad-token','browser-write-only-token')]:
                            cdp.evaluate(f"{{const e=document.getElementById({json.dumps(field)});e.value={json.dumps(value)};e.dispatchEvent(new Event('input',{{bubbles:true}}));}}")
                        self.assertTrue(cdp.evaluate("!document.querySelector('#submitter-enabled').disabled"))
                        cdp.click("[...document.querySelectorAll('#page button')].find(button=>button.textContent==='Save submitter')")
                        wait("document.querySelector('#page').innerText.includes('Submitter configuration saved.')")
                        wait("window.__submitterViewRead?.then(view=>view.config.mode==='forcad' && !view.config.enabled && view.config.forcad_ip==='127.0.0.1' && view.config.forcad_port===8080 && view.config.team_token_configured)")
                        self.assertEqual(cdp.evaluate("document.querySelector('#submitter-forcad-token').value"),'')
                        self.assertEqual(cdp.evaluate("document.querySelector('#submitter-forcad-token').placeholder"),'Saved token; leave blank to keep it')
                        self.assertNotIn('browser-write-only-token',cdp.evaluate("document.querySelector('#page').innerText"))
                        (artifacts / 'hydra-forcad-submit-ui.png').write_bytes(base64.b64decode(cdp.call('Page.captureScreenshot',{'format':'png'} )['data']))
                        cdp.evaluate("{const e=document.querySelector('#submitter-forcad-port');e.value='8081';e.dispatchEvent(new Event('input',{bubbles:true}));}")
                        cdp.click("[...document.querySelectorAll('#page button')].find(button=>button.textContent==='Save submitter')")
                        wait("document.querySelector('#page').innerText.includes('Submitter configuration saved.')")
                        wait("window.__submitterViewRead?.then(view=>view.config.forcad_port===8081 && view.config.team_token_configured && !view.config.enabled)")
                        cdp.click("[...document.querySelectorAll('#page [role=combobox]')].find(button=>button.textContent.trim()==='ForcAD')")
                        wait("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(option=>option.textContent.trim()==='Custom Python script')")
                        cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(option=>option.textContent.trim()==='Custom Python script')")
                        wait("Boolean(document.querySelector('#submitter-code[role=textbox][aria-multiline=true]'))")
                        self.assertEqual(cdp.evaluate("document.querySelector('#submitter-code').getAttribute('aria-label')"),'Custom Python submitter code')
                        custom_source = '# Submitter UTF-8: λ 🚩\n\nimport sys\n\nflags = sys.argv[1:]\n'
                        replace_editor('#submitter-code',custom_source)
                        wait("document.querySelector('#submitter-code')?.innerText.includes('Submitter UTF-8: λ 🚩') && document.querySelector('#submitter-code')?.innerText.includes('flags = sys.argv[1:]')")
                        (artifacts / 'hydra-submitter-editor.png').write_bytes(base64.b64decode(cdp.call('Page.captureScreenshot',{'format':'png'})['data']))
                        cdp.evaluate("window.__submitterSaveGate=new Promise(resolve=>window.__releaseSubmitterSave=resolve);const originalFetch=window.fetch;window.fetch=async(...args)=>{if(String(args[0]).endsWith('/api/v1/submitter')&&args[1]?.method==='PUT'){window.fetch=originalFetch;await window.__submitterSaveGate;}return originalFetch(...args)}")
                        cdp.click("[...document.querySelectorAll('#page button')].find(button=>button.textContent==='Save submitter')")
                        wait("document.querySelector('#submitter-code').getAttribute('aria-disabled')==='true' && document.querySelector('#submitter-code').getAttribute('contenteditable')==='false'")
                        cdp.click("document.querySelector('#submitter-code')")
                        cdp.call('Input.insertText',{'text':'blocked-edit'})
                        cdp.call('Input.dispatchKeyEvent',{'type':'keyDown','key':'Control','code':'ControlLeft','modifiers':2})
                        cdp.call('Input.dispatchKeyEvent',{'type':'keyDown','key':'z','code':'KeyZ','modifiers':2})
                        cdp.call('Input.dispatchKeyEvent',{'type':'keyUp','key':'z','code':'KeyZ','modifiers':2})
                        cdp.call('Input.dispatchKeyEvent',{'type':'keyUp','key':'Control','code':'ControlLeft'})
                        self.assertIn('Submitter UTF-8: λ 🚩',cdp.evaluate("document.querySelector('#submitter-code').innerText"))
                        self.assertNotIn('blocked-edit',cdp.evaluate("document.querySelector('#submitter-code').innerText"))
                        cdp.evaluate('window.__releaseSubmitterSave()')
                        wait("document.querySelector('#page').innerText.includes('Submitter configuration saved.')")
                        wait(f"window.__submitterViewRead?.then(view=>view.config.mode==='custom' && view.config.source==={json.dumps(custom_source)} && !view.config.enabled)")
                        self.assertTrue(cdp.evaluate("['Submission attempts','Flags','Submissions'].every(title=>[...document.querySelectorAll('#page button[aria-expanded]')].find(e=>e.textContent.trim().startsWith(title))?.getAttribute('aria-expanded')==='false')"))
                        self.assertFalse(cdp.evaluate("Boolean(document.querySelector('#flag-search') || document.querySelector('[aria-label=\"Flags per page\"]'))"))
                        for title in ['Submission attempts','Flags']:
                            cdp.click(f"[...document.querySelectorAll('#page button[aria-expanded]')].find(e=>e.textContent.trim().startsWith({json.dumps(title)}))")
                        wait("Boolean(document.querySelector('#flag-paste') && document.querySelector('#submitter-manual-count'))")
                        wait("document.querySelector('#page').innerText.includes('submission stdout FLAG002000') && document.querySelector('#page').innerText.includes('submission stderr FLAG002000')")
                        self.assertIn('Input flags (exact arguments)',cdp.evaluate("document.querySelector('#page').innerText"))
                        pasted=' '.join('FLAG%06d' % n for n in range(1000,1027))
                        cdp.evaluate(f"{{const e=document.querySelector('#flag-paste');e.value={json.dumps(pasted)};e.dispatchEvent(new Event('input',{{bubbles:true}}));}}")
                        cdp.evaluate("document.querySelector('#flag-paste').closest('form').requestSubmit()")
                        wait("document.querySelector('#page').innerText.includes('27 matches · 27 new flags')")
                        self.assertFalse(cdp.evaluate("[...document.querySelectorAll('#page table th')].some(e=>e.textContent==='Digest')"))
                        cdp.evaluate("(()=>{const button=[...document.querySelectorAll('#page button[aria-expanded]')].find(b=>b.innerText.trim().startsWith('Flags'));if(button?.getAttribute('aria-expanded')==='false')button.click()})()")
                        wait("Boolean(document.querySelector('[aria-label=\"Flags per page\"]'))")
                        open_select("document.querySelector('[aria-label=\\\"Sort flags\\\"]')",'Flag text')
                        wait("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(e=>e.textContent.trim()==='Flag text')")
                        cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==='Flag text')")
                        open_select("document.querySelector('[aria-label=\\\"Flag sort order\\\"]')",'Ascending')
                        wait("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(e=>e.textContent.trim()==='Ascending')")
                        cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==='Ascending')")
                        wait("document.querySelector('#page').innerText.includes('FLAG001000')")
                        open_select("document.querySelector('[aria-label=\\\"Flags per page\\\"]')",'25')
                        wait("document.querySelectorAll('[role=listbox][data-state=open] [role=option]').length===4")
                        cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==='25')")
                        wait("[...document.querySelectorAll('#page table')].find(t=>[...t.querySelectorAll('th')].some(h=>h.textContent==='Flag'))?.querySelectorAll('tbody tr').length===25")
                        wait("[...document.querySelectorAll('#page button')].some(b=>b.textContent==='Next'&&!b.disabled)")
                        cdp.evaluate("[...document.querySelectorAll('#page button')].find(b=>b.textContent==='Next').click()")
                        wait("document.querySelector('#page').innerText.includes('Page 2') && [...document.querySelectorAll('#page table')].find(t=>[...t.querySelectorAll('th')].some(h=>h.textContent==='Flag'))?.querySelectorAll('tbody tr').length===2")
                self.assertIn('alice',cdp.evaluate("document.querySelector('#page').innerText"))
                # A real nc-style prompt exchange renders as a terminal while live and after closure.
                with socket.socket() as terminal_backend:
                    terminal_backend.bind(('127.0.0.1',0));terminal_backend.listen();terminal_backend.settimeout(10)
                    terminal_done=threading.Event()
                    def terminal_reply():
                        try:
                            connection,_=terminal_backend.accept()
                            with connection:
                                connection.settimeout(5);connection.sendall(b'\x1b[32mName: \x1b[0m')
                                value=b''
                                while b'\n' not in value:
                                    part=connection.recv(128)
                                    if not part:return
                                    value+=part
                                if value.split(b'\n',1)[0]==b'alice':
                                    connection.sendall(b'Loading 1%\r\x1b[2KWelcome, \xc3');time.sleep(.03);connection.sendall(b'\xa9!\nbinary: \xff\n')
                                terminal_done.wait(5)
                        except OSError:pass
                    threading.Thread(target=terminal_reply,daemon=True).start()
                    cdp.evaluate("[...document.querySelectorAll('nav button')].find(b=>b.textContent==='Services').click()")
                    wait("[...document.querySelectorAll('#page button')].some(b=>b.textContent==='Add service'&&!b.disabled)")
                    cdp.click("[...document.querySelectorAll('#page button')].find(b=>b.textContent==='Add service')")
                    wait("Boolean(document.querySelector('#service-form'))")
                    nc_service={**service,'name':'Browser terminal','protocol_id':'utf8','listen_address':f'127.0.0.1:{port()}','upstream_address':f'127.0.0.1:{terminal_backend.getsockname()[1]}'}
                    fill_service(nc_service)
                    with sqlite3.connect(tmp/'db') as database:
                        nc_id=next(item['id'] for item in json.loads(database.execute('SELECT services FROM control_state').fetchone()[0]) if item['listen_address']==nc_service['listen_address'])
                    cdp.evaluate("[...document.querySelectorAll('nav button')].find(b=>b.textContent==='Traffic').click()")
                    wait(TRAFFIC_LIVE_EXPRESSION)
                    with socket.create_connection(('127.0.0.1',int(nc_service['listen_address'].split(':')[1])),timeout=5) as nc_client:
                        nc_client.settimeout(5);self.assertIn(b'Name:',nc_client.recv(128))
                        selector=f'#traffic-sessions [data-service-id="{nc_id}"]'
                        wait(f'Boolean(document.querySelector({json.dumps(selector)}))');cdp.click(f'document.querySelector({json.dumps(selector)})')
                        wait("document.querySelector('#traffic-terminal')?.innerText.includes('Name:')")
                        self.assertIn('Stream · terminal',cdp.evaluate("document.querySelector('[aria-label=\"Stream view\"]').textContent"))
                        # Leave an unfinished OSC command in client input. It
                        # must not consume the following server response.
                        nc_client.sendall(b'alice\n\x1b]unfinished');wait("document.querySelector('#traffic-terminal')?.innerText.includes('Welcome, é!')")
                        wait("document.querySelector('#traffic-terminal')?.innerText.includes('[ff]')")
                        screen=cdp.evaluate("document.querySelector('#traffic-terminal').innerText")
                        self.assertIn('Name: alice',screen);self.assertNotIn('Loading 1%',screen);self.assertNotIn('utf8.text:',screen)
                        self.assertLess(screen.index('Name:'),screen.index('Welcome,'))
                        self.assertEqual(cdp.evaluate("getComputedStyle(document.querySelector('#traffic-terminal .xterm-scrollable-element')).backgroundColor"), 'rgb(0, 0, 0)')
                        cdp.click("document.querySelector('#theme-toggle')")
                        wait("getComputedStyle(document.querySelector('#traffic-terminal .xterm-scrollable-element')).backgroundColor==='rgb(21, 21, 21)'")
                        cdp.click("document.querySelector('#theme-toggle')")
                        wait("getComputedStyle(document.querySelector('#traffic-terminal .xterm-scrollable-element')).backgroundColor==='rgb(0, 0, 0)'")
                        (artifacts / 'hydra-terminal-desktop.png').write_bytes(base64.b64decode(cdp.call('Page.captureScreenshot',{'format':'png'})['data']))
                        terminal_done.set()
                    wait("document.querySelector('#traffic-stream').innerText.includes('Closed')")
                    wait("document.querySelector('#traffic-terminal')?.innerText.includes('Welcome, é!') && document.querySelector('#traffic-terminal')?.innerText.includes('[ff]')")
                    for label in ['Decoded conversation','Captured bytes','Stream · terminal']:
                        open_select("document.querySelector('[aria-label=\\\"Stream view\\\"]')",label)
                        wait(f"[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(e=>e.textContent.trim()==={json.dumps(label)})")
                        cdp.click(f"[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==={json.dumps(label)})")
                        if label=='Decoded conversation':
                            # Core now ships a display_text summary, so the card shows
                            # the decoded text rather than the old `field: value`
                            # fallback; the typed field is still exposed in Inspect.
                            wait("document.querySelector('#traffic-stream').innerText.includes('Name: ')")
                            cdp.evaluate("(()=>{const card=document.querySelector('#traffic-stream article[data-direction]');[...card.querySelectorAll('button')].find(b=>b.textContent==='Inspect fields').click()})()")
                            wait("[...document.querySelectorAll('#traffic-stream article[data-direction] table tbody tr')].some(r=>r.cells[0]?.textContent==='utf8.text')")
                        elif label=='Captured bytes':wait("document.querySelector('#traffic-stream').innerText.includes('1b 5b 33 32')")
                        else:wait("document.querySelector('#traffic-terminal')?.innerText.includes('Welcome, é!')")
                # A new raw TCP history keeps cross-direction read order and
                # split UTF-8 bytes without changing the captured hex bytes.
                with socket.socket() as ordered_backend:
                    ordered_backend.bind(('127.0.0.1',0));ordered_backend.listen();ordered_backend.settimeout(10)
                    ordered_done=threading.Event();ordered_error=[]
                    def ordered_reply():
                        try:
                            peer,_=ordered_backend.accept()
                            with peer:
                                peer.settimeout(5);received=b''
                                while len(received)<2:
                                    part=peer.recv(128)
                                    if not part:return
                                    received+=part
                                if received!=b'A\xc3':raise AssertionError(f'first TCP chunk: {received!r}')
                                peer.sendall(b'B')
                                received=b''
                                while len(received)<2:
                                    part=peer.recv(128)
                                    if not part:return
                                    received+=part
                                if received!=b'\xa9C':raise AssertionError(f'second TCP chunk: {received!r}')
                                peer.sendall(b'D')
                        except (AssertionError,OSError) as error:ordered_error.append(str(error))
                        finally:ordered_done.set()
                    threading.Thread(target=ordered_reply,daemon=True).start()
                    cdp.evaluate("window.__hydraBrowserBearer='';const hydraOriginalFetch=window.fetch.bind(window);window.fetch=(input,init={})=>{const authorization=new Headers(init.headers).get('Authorization');if(authorization?.startsWith('Bearer '))window.__hydraBrowserBearer=authorization;return hydraOriginalFetch(input,init)}")
                    cdp.evaluate("[...document.querySelectorAll('nav button')].find(b=>b.textContent==='Services').click()")
                    wait("[...document.querySelectorAll('#page button')].some(b=>b.textContent==='Add service'&&!b.disabled)")
                    wait("Boolean(window.__hydraBrowserBearer)")
                    cdp.click("[...document.querySelectorAll('#page button')].find(b=>b.textContent==='Add service')")
                    wait("Boolean(document.querySelector('#service-form'))")
                    ordered_service={**service,'name':'Browser ordered raw','protocol_id':'raw','listen_address':f'127.0.0.1:{port()}','upstream_address':f'127.0.0.1:{ordered_backend.getsockname()[1]}'}
                    fill_service(ordered_service)
                    with sqlite3.connect(tmp/'db') as database:
                        ordered_id=next(item['id'] for item in json.loads(database.execute('SELECT services FROM control_state').fetchone()[0]) if item['listen_address']==ordered_service['listen_address'])
                    cdp.evaluate("[...document.querySelectorAll('nav button')].find(b=>b.textContent==='Traffic').click()")
                    wait(TRAFFIC_LIVE_EXPRESSION)
                    cdp.click("document.querySelector('#traffic-service')")
                    wait(f"[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(e=>e.textContent.trim()==={json.dumps(ordered_service['name'])})")
                    cdp.click(f"[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==={json.dumps(ordered_service['name'])})")
                    with socket.create_connection(('127.0.0.1',int(ordered_service['listen_address'].split(':')[1])),timeout=5) as ordered_client:
                        ordered_client.settimeout(5);ordered_client.sendall(b'A\xc3');self.assertEqual(ordered_client.recv(1),b'B')
                        ordered_client.sendall(b'\xa9C');self.assertEqual(ordered_client.recv(1),b'D')
                    self.assertTrue(ordered_done.wait(5), 'ordered TCP backend did not finish')
                    self.assertEqual(ordered_error,[])
                    selector=f'#traffic-sessions [data-service-id="{ordered_id}"]'
                    wait(f'Boolean(document.querySelector({json.dumps(selector)}))')
                    cdp.click(f'document.querySelector({json.dumps(selector)})')
                    wait("document.querySelector('#traffic-stream').innerText.includes('Closed')")
                    wait("document.querySelector('#traffic-stream').innerText.includes('chunk #4')")
                    cdp.click("document.querySelector('[aria-label=\"Stream view\"]')")
                    wait("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(e=>e.textContent.trim()==='Decoded conversation')")
                    cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==='Decoded conversation')")
                    wait("document.querySelectorAll('#traffic-stream article[data-direction]').length===4")
                    decoded=cdp.evaluate("[...document.querySelectorAll('#traffic-stream article[data-direction]')].map(e=>[e.dataset.direction,e.querySelector('pre')?.textContent])")
                    self.assertEqual(decoded,[['1','A'],['2','B'],['1','éC'],['2','D']])
                    self.assertIn('chunk #1',cdp.evaluate("document.querySelector('#traffic-stream').innerText"))
                    (artifacts / 'hydra-ordered-raw-desktop.png').write_bytes(base64.b64decode(cdp.call('Page.captureScreenshot',{'format':'png'})['data']))
                    cdp.click("document.querySelector('[aria-label=\"Stream view\"]')")
                    wait("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(e=>e.textContent.trim()==='Captured bytes')")
                    cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==='Captured bytes')")
                    cdp.click("document.querySelector('[aria-label=\"Raw byte format\"]')")
                    wait("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(e=>e.textContent.trim()==='UTF-8')")
                    cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==='UTF-8')")
                    wait("document.querySelectorAll('#traffic-stream article[data-direction]').length===4")
                    utf8=cdp.evaluate("[...document.querySelectorAll('#traffic-stream article[data-direction]')].map(e=>[e.dataset.direction,e.querySelector('pre')?.textContent])")
                    self.assertEqual(utf8,[['1','A'],['2','B'],['1','éC'],['2','D']])
                    self.assertNotIn('�',str(utf8))
                    cdp.click("document.querySelector('[aria-label=\"Raw byte format\"]')")
                    wait("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(e=>e.textContent.trim()==='Hex dump')")
                    cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==='Hex dump')")
                    hex_chunks=cdp.evaluate("[...document.querySelectorAll('#traffic-stream article[data-direction]')].map(e=>[e.dataset.direction,e.querySelector('pre')?.textContent,e.innerText])")
                    self.assertEqual([chunk[0] for chunk in hex_chunks],['1','2','1','2'])
                    for chunk,prefix in zip(hex_chunks,['00000000  41 c3','00000000  42','00000000  a9 43','00000000  44']):self.assertTrue(chunk[1].startswith(prefix),chunk[1])
                    for index,chunk in enumerate(hex_chunks,1):self.assertIn(f'chunk #{index}',chunk[2])
                    ordered_flow_id=cdp.evaluate("document.querySelector('#traffic-sessions [aria-pressed=true]')?.dataset.flowId")
                    self.assertTrue(ordered_flow_id)
                    cdp.evaluate("[...document.querySelectorAll('nav button')].find(b=>b.textContent==='Services').click()")
                    wait("[...document.querySelectorAll('#page button')].some(b=>b.textContent==='Add service'&&!b.disabled)")
                    wait("Boolean(window.__hydraBrowserBearer)")
                    profile_update=cdp.evaluate(f"""(async()=>{{
                      const authorization=window.__hydraBrowserBearer;
                      const currentResponse=await fetch('/api/v1/services',{{headers:{{Authorization:authorization}}}});
                      if(!currentResponse.ok)return {{status:currentResponse.status}};
                      const current=await currentResponse.json();
                      const service=current.items.find(item=>item.id==={json.dumps(ordered_id)});
                      if(!service)return {{status:404}};
                      const body={{name:service.name,expected_revision:current.revision,transport:service.transport,listen_address:service.listen_address,upstream_address:service.upstream_address,protocol_id:'utf8',enabled:service.enabled,retain_history:service.retain_history,forwarding:service.forwarding??null,protection:service.protection??null}};
                      const response=await fetch(`/api/v1/services/${{encodeURIComponent(service.id)}}`,{{method:'PATCH',headers:{{Authorization:authorization,'Content-Type':'application/json'}},body:JSON.stringify(body)}});
                      const result=await response.json();
                      return {{status:response.status,revision:result.services?.revision,protocol_id:result.services?.services?.find(item=>item.id===service.id)?.protocol_id}};
                    }})()""")
                    self.assertEqual(profile_update['status'],202,profile_update)
                    self.assertEqual(profile_update['protocol_id'],'utf8',profile_update)
                    # Traffic rows, selection metadata and flow hydration now arrive
                    # over the live WebSocket, so the lost-closure fault is injected
                    # at the message boundary for this exact flow only. The native
                    # socket class is subclassed: auth/control frames, OPEN
                    # constants, send/close and the prototype stay untouched; a
                    # changed frame is redispatched once (guarded by `changed`).
                    # The existing Page.reload above already cleared the older
                    # in-page probes; there is no HTTP flow path to patch anymore.
                    lost_closure_script=f"""(()=>{{
                      const targetFlow={json.dumps(ordered_flow_id)};
                      const NativeSocket=window.WebSocket;
                      window.__lostClosureFixtureApplied=false;
                      class LostClosureSocket extends NativeSocket {{
                        constructor(...args){{
                          super(...args);
                          this.addEventListener('message',event=>{{
                            const data=event.data;
                            if(typeof data!=='string'||!data.includes(targetFlow))return;
                            let frame;try{{frame=JSON.parse(data)}}catch{{return}}
                            let changed=false;
                            const stripClosed=item=>{{if(item&&typeof item==='object'&&item.flow_id===targetFlow&&Object.hasOwn(item,'closed')){{delete item.closed;changed=true}}}};
                            if(Array.isArray(frame.items))frame.items.forEach(stripClosed);
                            stripClosed(frame.flow);
                            if(!changed)return;
                            event.stopImmediatePropagation();
                            window.__lostClosureFixtureApplied=true;
                            this.dispatchEvent(new MessageEvent('message',{{data:JSON.stringify(frame)}}));
                          }},true);
                        }}
                      }}
                      window.WebSocket=LostClosureSocket;
                    }})()"""
                    cdp.call('Page.reload')
                    wait("document.querySelector('#console') && !document.querySelector('#console').hidden")
                    cdp.evaluate(lost_closure_script)
                    cdp.evaluate("[...document.querySelectorAll('nav button')].find(b=>b.textContent==='Traffic').click()")
                    wait(TRAFFIC_LIVE_EXPRESSION)
                    cdp.click("document.querySelector('#traffic-service')")
                    wait(f"[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(e=>e.textContent.trim()==={json.dumps(ordered_service['name'])})")
                    cdp.click(f"[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==={json.dumps(ordered_service['name'])})")
                    selector=f'#traffic-sessions [data-flow-id="{ordered_flow_id}"]'
                    wait(f'Boolean(document.querySelector({json.dumps(selector)}))')
                    wait('window.__lostClosureFixtureApplied===true')
                    cdp.click(f'document.querySelector({json.dumps(selector)})')
                    wait("document.querySelector('#traffic-stream').innerText.includes('Proxy has a saved capture; Core still lists this flow as open.')")
                    self.assertIn('Open',cdp.evaluate(f"document.querySelector({json.dumps(selector)}).innerText"))
                    self.assertIn('4 B sent / 2 B received',cdp.evaluate("document.querySelector('#traffic-stream').innerText"))
                    self.assertNotIn('Connection ended:',cdp.evaluate("document.querySelector('#traffic-stream').innerText"))
                    cdp.click("document.querySelector('[aria-label=\"Stream view\"]')")
                    wait("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(e=>e.textContent.trim()==='Decoded conversation')")
                    cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==='Decoded conversation')")
                    wait("document.querySelectorAll('#traffic-stream article[data-direction]').length===4")
                    utf8_history=cdp.evaluate("[...document.querySelectorAll('#traffic-stream article[data-direction]')].map(e=>[e.dataset.direction,e.querySelector('pre')?.textContent])")
                    self.assertEqual(utf8_history,[['1','A'],['2','B'],['1','éC'],['2','D']])
                    raw_metadata=cdp.evaluate("(()=>{const details=document.querySelector('#traffic-stream details');return details&&{summary:details.querySelector('summary')?.textContent,open:details.open}})()")
                    self.assertEqual(raw_metadata,{'summary':'Raw metadata · 2 read summaries','open':False})
                    cdp.click("document.querySelector('#traffic-stream details summary')")
                    wait("Boolean(document.querySelector('#traffic-stream details[open] table tbody tr'))")
                # Real UDP traffic is visible without switching away from the default list.
                with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as udp_backend:
                    udp_backend.bind(('127.0.0.1',0));udp_backend.settimeout(10)
                    def udp_reply():
                        try:
                            value,peer=udp_backend.recvfrom(65535)
                            if value==b'UDP browser request':udp_backend.sendto(b'UDP browser response',peer)
                        except OSError:pass
                    cdp.evaluate("[...document.querySelectorAll('nav button')].find(b=>b.textContent==='Services').click()")
                    wait("[...document.querySelectorAll('#page button')].some(b=>b.textContent==='Add service'&&!b.disabled)")
                    cdp.click("[...document.querySelectorAll('#page button')].find(b=>b.textContent==='Add service')")
                    wait("Boolean(document.querySelector('#service-transport'))")
                    cdp.click("document.querySelector('#service-transport')")
                    wait("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(e=>e.textContent.trim()==='UDP')")
                    cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent.trim()==='UDP')")
                    udp_service={**service,'name':'Browser UDP','transport':'udp','protocol_id':'utf8','listen_address':f'127.0.0.1:{port()}','upstream_address':f'127.0.0.1:{udp_backend.getsockname()[1]}'}
                    fill_service(udp_service)
                    cdp.evaluate("[...document.querySelectorAll('nav button')].find(b=>b.textContent==='Traffic').click()")
                    wait(TRAFFIC_LIVE_EXPRESSION)
                    threading.Thread(target=udp_reply,daemon=True).start()
                    with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as udp_client:
                        udp_client.settimeout(3);udp_client.sendto(b'UDP browser request',('127.0.0.1',int(udp_service['listen_address'].split(':')[1])))
                        self.assertEqual(udp_client.recv(65535),b'UDP browser response')
                    wait("Boolean(document.querySelector('#traffic-sessions [data-transport=\"2\"]'))")
                    cdp.click("document.querySelector('#traffic-sessions [data-transport=\"2\"]')")
                    wait("document.querySelector('#traffic-stream').innerText.includes('UDP browser request') && document.querySelector('#traffic-stream').innerText.includes('UDP browser response')")
                    self.assertIn('Follow UDP conversation',cdp.evaluate("document.querySelector('#traffic-stream h2').innerText"))
                    wait("document.querySelector('#traffic-stream').innerText.includes('Closed')")
                    cdp.click("document.querySelector('[aria-label=\"Stream view\"]')")
                    wait("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].some(e=>e.textContent==='Captured bytes')")
                    cdp.click("[...document.querySelectorAll('[role=listbox][data-state=open] [role=option]')].find(e=>e.textContent==='Captured bytes')")
                    wait("document.querySelector('#traffic-stream').innerText.includes('2 datagrams')")
                    saved=cdp.evaluate("document.querySelector('#traffic-stream').innerText")
                    self.assertIn('datagram #1',saved);self.assertIn('datagram #2',saved)
                    self.assertIn('55 44 50',saved)
                    self.assertNotIn('Saved stream history is temporarily unavailable',saved)
                # Proxy disconnect is a real child-process stop, with Core/UI live.
                cdp.evaluate("[...document.querySelectorAll('nav button')].find(b=>b.textContent==='Traffic').click()")
                wait(TRAFFIC_LIVE_EXPRESSION)
                processes[0].terminate();processes[0].wait(timeout=10)
                wait("document.querySelector('#hydra-header').dataset.proxyConnected==='false'",seconds=12)
                self.assertTrue(cdp.evaluate("document.querySelector('#hydra-header').classList.contains('bg-red-950')"))
                self.assertIn('Proxy disconnected',cdp.evaluate("document.querySelector('#status').textContent"))
                # Save while disconnected; reconnection must apply the latest value.
                cdp.evaluate("[...document.querySelectorAll('nav button')].find(b=>b.textContent==='Settings').click()")
                wait("Boolean(document.querySelector('#settings-history-limit'))")
                cdp.evaluate("{const e=document.querySelector('#settings-history-limit');e.value='3';e.dispatchEvent(new Event('input',{bubbles:true}));}")
                cdp.evaluate("document.querySelector('#settings-form').requestSubmit()")
                wait("document.querySelector('#page').innerText.includes('Saved to the configuration file.')")
                self.assertEqual(json.loads((tmp/'operations.json').read_text())['traffic_history_per_service_mib'],3)
                processes.append(subprocess.Popen([str(BIN/'hydra-proxy'),'--config',str(tmp/'proxy.json')],stdout=log,stderr=log))
                wait("document.querySelector('#hydra-header').dataset.proxyConnected==='true'",seconds=12)
                self.assertFalse(cdp.evaluate("document.querySelector('#hydra-header').classList.contains('bg-red-950')"))
                status=json.loads(subprocess.check_output([str(BIN/'hydra-core'),'--socket',sock,'status'],text=True))
                self.assertEqual(status['history_per_service_bytes'],3*1024*1024)

                cdp.evaluate("document.querySelector('#logout').click()")
                wait("document.querySelector('#login') && !document.querySelector('#login').hidden")
                self.assertNotIn('hydra_csrf=',cdp.evaluate('document.cookie'))
                cdp.socket.close()
                print('Embedded Vue, native HTTPS and real Chromium login, navigation, API calls, logout: OK')
            finally:
                if upstream:upstream.close()
                if occupied:occupied.close()
                retry_backend_stop.set()
                if retry_backend:retry_backend.close()
                if cdp:
                    cdp.socket.close()
                try:
                    for p in reversed(processes):
                        if p.poll() is None:
                            p.terminate()
                        try:
                            p.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            p.kill()
                            p.wait(timeout=5)
                finally:
                    if hidden_dist is not None:
                        rename_dist_preserving_parent_mtime(hidden_dist, dist)
                    log.close()


if __name__ == '__main__':
    unittest.main()
