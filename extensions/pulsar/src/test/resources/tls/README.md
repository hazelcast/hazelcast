These certificates and the private key are test fixtures only. The broker certificate is
self-signed and trusted explicitly by the TLS tests. Its subject alternative names cover
`localhost`, `127.0.0.1`, and `::1`. The unrelated certificate exercises certificate rejection.
Both certificates expire in 2126.

Regenerate with OpenSSL (the unrelated private key is discarded):

```sh
openssl req -x509 -newkey rsa:2048 -nodes -keyout broker-key.pem -out broker-cert.pem \
  -days 36500 -subj '/CN=localhost' \
  -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1'
openssl req -x509 -newkey rsa:2048 -nodes -keyout untrusted-key.pem -out untrusted-cert.pem \
  -days 36500 -subj '/CN=untrusted'
rm untrusted-key.pem
```
