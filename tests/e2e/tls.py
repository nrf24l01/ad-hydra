"""TLS termination and upstream TLS through the real Proxy and Core binaries."""
import http.client
import json
import pathlib
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from relay import BIN, port  # noqa: E402

ORIGIN = 'https://hydra.test'


def self_signed(directory):
    certificate = directory / 'tls.crt'
    key = directory / 'tls.key'
    subprocess.run(
        ['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
         '-keyout', str(key), '-out', str(certificate), '-subj', '/CN=localhost',
         '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1'],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return certificate.read_text(), key.read_text()


def tls_echo(server):
    """Accept one TLS client at a time and echo decrypted bytes back."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(server['certificate'], server['key'])

    def serve():
        while True:
            try:
                raw, _ = server['socket'].accept()
            except OSError:
                return
            try:
                with context.wrap_socket(raw, server_side=True) as conn:
                    while True:
                        data = conn.recv(65536)
                        if not data:
                            break
                        conn.sendall(b'upstream-tls:' + data)
            except (ssl.SSLError, OSError):
                pass
            finally:
                raw.close()

    threading.Thread(target=serve, daemon=True).start()


class Tls(unittest.TestCase):
    def test_termination_and_upstream_tls(self):
        with tempfile.TemporaryDirectory(prefix='hydra-tls-') as directory:
            tmp = pathlib.Path(directory)
            certificate, key = self_signed(tmp)
            backend = socket.socket()
            backend.bind(('127.0.0.1', 0))
            backend.listen(8)
            tls_echo({'socket': backend, 'certificate': str(tmp / 'tls.crt'),
                      'key': str(tmp / 'tls.key')})
            ap, pp = port(), port()
            sock = str(tmp / 'proxy.sock')
            (tmp / 'proxy.json').write_text(json.dumps({
                'socket': sock, 'state_file': str(tmp / 'saved.json'),
                'snapshot': {'revision': 0, 'services': []}}))
            log = open(tmp / 'logs', 'w+')
            args = [str(BIN / 'hydra-core'), '--database', str(tmp / 'core.db'),
                    '--key', str(tmp / 'jwt.pem'), '--socket', sock]

            def request(method, path, payload=None, token=None):
                c = http.client.HTTPConnection('127.0.0.1', ap, timeout=5)
                headers = {'Origin': ORIGIN}
                if token:
                    headers['Authorization'] = 'Bearer ' + token
                if payload is not None:
                    headers['Content-Type'] = 'application/json'
                c.request(method, '/api/v1' + path,
                          json.dumps(payload) if payload is not None else None, headers)
                r = c.getresponse()
                raw = r.read()
                c.close()
                return r.status, json.loads(raw) if raw else None

            def good(method, path, payload=None, token=None):
                status, body = request(method, path, payload, token)
                self.assertIn(status, (200, 201, 202, 204), (status, body))
                return body

            processes = []
            try:
                subprocess.run(args + ['create', 'user', 'alice', 'test-password-1234'],
                               check=True, stdout=log, stderr=log, timeout=10)
                processes.append(subprocess.Popen(
                    [str(BIN / 'hydra-proxy'), '--config', str(tmp / 'proxy.json')],
                    stdout=log, stderr=log))
                processes.append(subprocess.Popen(
                    args + ['serve', '--listen', f'127.0.0.1:{ap}', '--origin', ORIGIN],
                    stdout=log, stderr=log))
                for _ in range(100):
                    try:
                        if request('GET', '/services')[0] == 401:
                            break
                    except (OSError, http.client.HTTPException):
                        pass
                    time.sleep(.05)
                else:
                    log.seek(0)
                    self.fail(log.read())

                token = good('POST', '/auth/login', {
                    'username': 'alice', 'password': 'test-password-1234'})['access_token']
                created = good('POST', '/services', {
                    'expected_revision': 0, 'transport': 'tcp',
                    'listen_address': f'127.0.0.1:{pp}',
                    'upstream_address': f'127.0.0.1:{backend.getsockname()[1]}',
                    'protocol_id': 'raw', 'enabled': True,
                    'tls': {'terminate': False, 'upstream': True, 'insecure': True,
                            'server_name': 'localhost'}}, token)
                service_id = created['services']['services'][0]['id']
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    if good('GET', '/system/proxy', token=token)['applied_services_revision'] == 1:
                        break
                    time.sleep(.05)

                # Begin with a plaintext listener, then enable termination on
                # the already-bound address. The acceptor must update live.
                with socket.create_connection(('127.0.0.1', pp), timeout=5) as client:
                    client.sendall(b'hello over plaintext')
                    self.assertEqual(client.recv(65536), b'upstream-tls:hello over plaintext')
                updated = good('PATCH', f'/services/{service_id}', {
                    'expected_revision': 1, 'transport': 'tcp',
                    'listen_address': f'127.0.0.1:{pp}',
                    'upstream_address': f'127.0.0.1:{backend.getsockname()[1]}',
                    'protocol_id': 'raw', 'enabled': True,
                    'tls': {'terminate': True, 'upstream': True, 'insecure': True,
                            'certificate_pem': certificate, 'private_key_pem': key,
                            'server_name': 'localhost'}}, token)
                self.assertNotIn('private_key_pem', json.dumps(updated))
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    if good('GET', '/system/proxy', token=token)['applied_services_revision'] == 2:
                        break
                    time.sleep(.05)

                # The private key must never come back from the API, on any route.
                self.assertNotIn('private_key_pem', json.dumps(created))
                listed = good('GET', '/services', token=token)['items'][0]
                self.assertEqual(listed['tls']['private_key_configured'], True)
                self.assertNotIn('private_key_pem', listed['tls'])
                exported = good('GET', '/config/export', token=token)
                self.assertNotIn('private_key_pem', json.dumps(exported))
                self.assertEqual(
                    exported['services']['services'][0]['tls']['private_key_configured'], True)

                # A plaintext client cannot reach the upstream through a TLS listener:
                # it gets a TLS alert or a closed socket, never the echoed payload.
                with socket.create_connection(('127.0.0.1', pp), timeout=3) as plain:
                    plain.sendall(b'not tls\r\n')
                    plain.settimeout(3)
                    try:
                        reply = plain.recv(64)
                    except (ConnectionResetError, socket.timeout):
                        reply = b''
                self.assertNotIn(b'upstream-tls', reply)

                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                with socket.create_connection(('127.0.0.1', pp), timeout=5) as raw:
                    with context.wrap_socket(raw, server_hostname='localhost') as client:
                        client.sendall(b'hello over tls')
                        echoed = client.recv(65536)
                self.assertEqual(echoed, b'upstream-tls:hello over tls')

                # Every mutation and export response also redacts the key.
                filter_response = good('POST', '/filters', {
                    'expected_revision': 2, 'service_id': service_id,
                    'name': 'tls stream marker', 'description': '', 'enabled': True,
                    'mode': 'enforce', 'priority': 1,
                    'expression': {'field': 'stream.text', 'op': 'regex',
                                   'value': {'type': 'string', 'value': 'never-match'}},
                    'action': {'type': 'drop_connection'}, 'backend': 'userspace',
                    'expires_at_ms': None}, token)
                self.assertNotIn('private_key_pem', json.dumps(filter_response))
                export = good('GET', '/config/export', token=token)
                self.assertNotIn('private_key_pem', json.dumps(export))

                # Inspected plaintext is indexed as a normal flow.
                flows = good('GET', '/traffic/flows', token=token)['items']
                self.assertTrue(any(f['upstream'] for f in flows), flows)

                # Replacing the certificate reaches a listener that is already open.
                (tmp / 'second').mkdir()
                second_certificate, second_key = self_signed(tmp / 'second')
                self.assertNotEqual(second_certificate, certificate)
                # Read the revision instead of assuming one: the earlier PATCH moved it.
                revision = good('GET', '/services', token=token)['revision']
                good('PATCH', f'/services/{created["id"]}', {
                    'expected_revision': revision, 'transport': 'tcp',
                    'listen_address': f'127.0.0.1:{pp}',
                    'upstream_address': f'127.0.0.1:{backend.getsockname()[1]}',
                    'protocol_id': 'raw', 'enabled': True,
                    'tls': {'terminate': True, 'upstream': True, 'insecure': True,
                            'certificate_pem': second_certificate,
                            'private_key_pem': second_key,
                            'server_name': 'localhost'}}, token)
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    if good('GET', '/system/proxy', token=token)['applied_services_revision'] == revision + 1:
                        break
                    time.sleep(.05)
                with socket.create_connection(('127.0.0.1', pp), timeout=5) as raw:
                    with context.wrap_socket(raw, server_hostname='localhost') as client:
                        served = client.getpeercert(binary_form=True)
                        client.sendall(b'after reload')
                        self.assertEqual(client.recv(65536), b'upstream-tls:after reload')
                self.assertEqual(served, ssl.PEM_cert_to_DER_cert(second_certificate))
            finally:
                for process in processes:
                    process.terminate()
                for process in processes:
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                backend.close()
                log.close()


if __name__ == '__main__':
    unittest.main()
