The TLS tests use OpenSSL in the Pulsar container to generate a self-signed broker
certificate for the host returned by Testcontainers. The unrelated certificate
in this directory exercises certificate rejection and expires in 2126.

Regenerate with OpenSSL (the unrelated private key is discarded):

```sh
openssl req -x509 -newkey rsa:2048 -nodes -keyout untrusted-key.pem -out untrusted-cert.pem \
  -days 36500 -subj '/CN=untrusted'
rm untrusted-key.pem
```
