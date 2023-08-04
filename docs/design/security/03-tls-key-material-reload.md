# Ability to reload the TLS key material

## Summary

Allow using configurable timeout for caching keyStores and trustStores
in the TLS configuration (`<ssl/>`).

## Goals

* adding `ssl` config property for configuring timeout (duration) for caching keyStore and trustStore;
* reloading key material when the key material duration passes;

## Non-Goals

* adding dynamic config (e.g. changing path to keyStores);
* handling broken keyStores (e.g. when a non-atomic content replace
  is in progress during the material reload);
* caching behavior for `keyFile`, `keyCertChainFile`, and `trustCertCollectionFile`
  in the `OpenSSLEngineFactory`.

## Motivation

Hazelcast Enterprise allows using TLS protocol for data in transit protection.
We want to enable key material (keyStores and trustStores) rotation without
needing a Hazelcast instance restart.

The current process required for the key material update is described in the
[Updating Certificates in the Running Cluster](https://docs.hazelcast.com/hazelcast/latest/security/tls-configuration#updating-certificates-in-the-running-cluster)
section of the official documentation. It includes:

* stopping each member;
* waiting for cluster safe state;
* updating key material;
* starting the member;
* waiting for cluster safe state

These steps might require to be repeated (1-3 times) - based on the certificates trust configuration,

## Functional design

New optional property, `keyMaterialDuration`, will be introduced into the `<ssl>`
properties config. The value will be a
[duration expression in ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations)
as supported by Java
[`java.time.Duration.parse()`](https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-)
method.

The `keyMaterialDuration` property value is a string such as `PnDTnHnMn.nS`.

The `Duration.parse()` JavaDoc describes the format as:

> The string starts with an optional sign, denoted by the ASCII negative or positive symbol. If negative, the whole period is negated. The ASCII letter "P" is next in upper or lower case. There are then four sections, each consisting of a number and a suffix. The sections have suffixes in ASCII of "D", "H", "M" and "S" for days, hours, minutes and seconds, accepted in upper or lower case. The suffixes must occur in order. The ASCII letter "T" must occur before the first occurrence, if any, of an hour, minute or second section. At least one of the four sections must be present, and if "T" is present there must be at least one section after the "T". The number part of each section must consist of one or more ASCII digits. The number may be prefixed by the ASCII negative or positive symbol. The number of days, hours and minutes must parse to an long. The number of seconds must parse to an long with optional fraction. The decimal point may be either a dot or a comma. The fractional part may have from zero to 9 digits.
>
> The leading plus/minus sign, and negative values for other units are not part of the ISO-8601 standard.

A positive `keyMaterialDuration` value (e.g. `PT1H`) says for how long should be the key material cached before it's newly loaded.

A negative `keyMaterialDuration` value (e.g. `PT-1s`) means the key material will be cached indefinitely.

A zero-value duration expression (e.g. `PT0s`) means the key material will not be cached and will always be newly loaded for each TLS-protected connection.

The key material is cached indefinitely if the new property is not specified (default value).
We keep the behavior backward-compatible.

If the value has a wrong format, the Hazelcast instance won't start.

The `OpenSSLEngineFactory` doesn't cache the key material when native key
and certificate files are used (`keyFile`, `keyCertChainFile`,
and `trustCertCollectionFile`). This behavior won't change.

### Sample configuration

The following configuration example will cache the key material for 10 minutes
before the new reload.

```xml
<network>
    <ssl enabled="true">
        <properties>
            <property name="keyMaterialDuration">PT10M</property>
            <property name="keyStore">${keyStore.path}</property>
            <property name="keyStorePassword">${keyStore.password}</property>
            <property name="trustStore">${trustStore.path}</property>
            <property name="trustStorePassword">${trustStore.password}</property>
            <property name="protocol">TLSv1.3</property>
            <property name="mutualAuthentication">REQUIRED</property>
        </properties>
    </ssl>
</network>
```

### Updated process steps

The process of replacing key material described in the Motivation section will be simplified to the following steps
when a non-negative `keyMaterialDuration` value is used.

#### 1. When new certificates are not trusted by the members

This is usually a case when self-signed certificates are used on the members.

Before we can deploy new member certificates, we have to update the list of trusted certificates on all members:

* Import all new certificates to every member trustStore, so it contains both old and new ones.
* Wait at least the time specified in the `keyMaterialDuration` parameter.
* After completing the above steps, follow the steps described in the next section (certificates trusted).

#### 2. When new certificates are already trusted by the members

Switch certificates/keys on all members:

* Replace the private key and certificate in every member keyStore.

At the latest, after the specified duration, all new connections will use the new key material.

#### 3. If the old certificates need to be removed

When the mutual TLS authentication is enabled, and there is a key leakage,
or the old certificates are not allowed to be used anymore for any reason,
the trustStores have to be updated once more.

This point is not described in the documentation, but it worked in the same way as point 1.
I.e. Prepare trustStores without old certificates and update trustStores on all members one by one.

Again this is simplified by setting a non-negative `keyMaterialDuration`.
At the latest, after the specified duration, new connections
with old certificates (used for mutual authentication) won't be allowed.

### Considered alternative approach

Another approach to deal with reloads would be reloading the material for every connection or introducing only the `true`/`false` flag
to enable/disable a non-expiring cache altogether. As these approaches could impact performance, we won't implement these alternatives.

## Technical design

The abstract class `com.hazelcast.internal.nio.ssl.SSLEngineFactorySupport`
in `hazelcast-enterprise` repository will be extended to support the new property.
We will also align the implementations (i.e. child classes).

Hazelcast sample full configuration will be extended. The property will be added to
`hazelcast-full-example` and `hazelcast-client-full-example` files.

## Testing

New tests covering the new functionality will be added to the `hazelcast-enterprise` repository.
