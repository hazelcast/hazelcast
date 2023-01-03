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
We want to allow key material (keyStores and trustStores) rotation without
necessity of Hazelcast instance restart.

## Functional design

New optional property `keyMaterialDuration` will be introduced into the `<ssl>`
properties config. The value will be a [duration expression in
ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601#Durations)
as supported by Java [`java.time.Duration.parse()`](https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-)
method.

The `keyMaterialDuration` property value is a string such as `PnDTnHnMn.nS`.

The `Duration.parse()` JavaDoc describes the format as:

> The string starts with an optional sign, denoted by the ASCII negative or positive symbol. If negative, the whole period is negated. The ASCII letter "P" is next in upper or lower case. There are then four sections, each consisting of a number and a suffix. The sections have suffixes in ASCII of "D", "H", "M" and "S" for days, hours, minutes and seconds, accepted in upper or lower case. The suffixes must occur in order. The ASCII letter "T" must occur before the first occurrence, if any, of an hour, minute or second section. At least one of the four sections must be present, and if "T" is present there must be at least one section after the "T". The number part of each section must consist of one or more ASCII digits. The number may be prefixed by the ASCII negative or positive symbol. The number of days, hours and minutes must parse to an long. The number of seconds must parse to an long with optional fraction. The decimal point may be either a dot or a comma. The fractional part may have from zero to 9 digits.
>
> The leading plus/minus sign, and negative values for other units are not part of the ISO-8601 standard.

A positive `keyMaterialDuration` value (e.g. `PT1H`) says for how long should be the key material cached before it's newly loaded.

A negative `keyMaterialDuration` value (e.g. `PT-1s`) means the key material will be cached indefinetely.

A zero-value duration expression (e.g. `PT0s`) means the key material will not be cached and it will be always newly loaded for each TLS protected connection.

If the new property is not specified (default value), the key material is cached indefinetely.
This keeps the backward compatible behavior.

If the value has a wrong format the Hazelcast instance won't start.

The `OpenSSLEngineFactory` doesn't cache the the key material when native key
and certificate files are used (`keyFile`, `keyCertChainFile`,
and `trustCertCollectionFile`). This behavior won't change.

### Sample configuration

The following configuration example will cache the key material for 10 minutes
before new reload.

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


### Considered alternative approach

Another approach how to deal with reloads would be to introducing only `true`/`false` flag
to enable/disable the cache completely. As this approach could have a bigger performance
impact this alternative won't be implemented.

## Technical design

The abstract class `com.hazelcast.internal.nio.ssl.SSLEngineFactorySupport`
in `hazelcast-enterprise` will be extended to support the new property.
Its implementations will be aligned. 

Hazelcast sample full configuration will be extended. The property will be added to
`hazelcast-full-example` and `hazelcast-client-full-example` files.

## Testing

New tests covering the new functionality will be added into the `hazelcast-enterprise` repository.
