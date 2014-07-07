
## Encryption

![](images/enterprise-onlycopy.jpg)

Hazelcast allows you to encrypt entire socket level communication among all Hazelcast members. Encryption is based on [Java Cryptography Architecture](http://java.sun.com/javase/6/docs/technotes/guides/security/crypto/CryptoSpec.html). In symmetric encryption, each node uses the same key, so the key is shared. Here is a sample configuration for symmetric encryption:

```xml
<hazelcast>
  ...
  <network>
    ...
    <!--
      Make sure to set enabled=true
      Make sure this configuration is exactly the same on
      all members
    -->
    <symmetric-encryption enabled="true">
      <!--
        encryption algorithm such as
        DES/ECB/PKCS5Padding,
        PBEWithMD5AndDES,
        Blowfish,
        DESede
      -->
      <algorithm>PBEWithMD5AndDES</algorithm>

      <!-- salt value to use when generating the secret key -->
      <salt>thesalt</salt>

      <!-- pass phrase to use when generating the secret key -->
      <password>thepass</password>

      <!-- iteration count to use when generating the secret key -->
      <iteration-count>19</iteration-count>
    </symmetric-encryption>
  </network>
  ...
</hazelcast>
```

<br> </br>


***RELATED INFORMATION***

*Please see [SSL](#ssl).*
