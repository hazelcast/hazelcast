---
title: 004 - Spring Boot Starter
description: Spring Boot Starter for Hazelcast Jet
---

*Since*: 4.1

## Background

User should be able to auto configure/start Jet in a Spring Boot
environment.

## Implementation

Spring Boot uses `META-INF/spring.factories` metadata file to define
classes which will be evaluated for auto-configuration. We've added
`HazelcastJetAutoConfiguration` class as a starting point for auto
configuring Jet.

```java
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(JetInstance.class)
@Import({ HazelcastJetServerConfiguration.class, HazelcastJetClientConfiguration.class })
public class HazelcastJetAutoConfiguration {
}
```

### ConfigurationProperties

Spring Boot has a feature named `ConfigurationProperties` which makes
external configuration easily accessible. We've defined configuration
properties for server and client. User can point the starter to the
desired configuration file by defining `hazelcast.jet.server.config`
for server, `hazelcast.jet.imdg.config` for imdg, and
`hazelcast.jet.client.config` for client.

### Conditional

Spring Boot has a concept of `Conditional`, which can be put on to the
classes as an annotation. The conditional enables the evaluation of the
class if the defined condition is met. There are some predefined
conditionals like `@ConditionalOnClass` or `@ConditionalOnMissingBean`.
You can also create a custom `Condition` and use it with `@Conditional`
annotation.

We've created custom conditions for server and client configurations.
These conditions checks if a configuration file is defined, as a system
property or a [configuration property](#configurationproperties). If
not the conditions looks for the configuration files
( `hazelcast-jet.(yaml|yml|xml)` for server and
`hazelcast-client.(yaml|yml|xml)` for client ) on the classpath or at
the root directory.

### The Order of Condition Check Flow

Since `HazelcastJetAutoConfiguration` imports the server configuration
first and then the client, Spring Boot evaluates server configuration
class first.

- If `JetConfig` is available as a bean, a member will be created using
  the bean.
  
- If `ClientConfig` is available as a bean, a client will be created
  using the bean.

- If `hazelcast.jet.server.config` config property is defined, a member
  will be created using the defined configuration file.

- If `hazelcast.jet.config` system property is defined, a member will
  be created using the defined configuration file.

- If `hazelcast.jet.client.config` config property is defined, a client
  will be created using the defined configuration file.

- If `hazelcast.client.config` system property is defined, a client
  will be created using the defined configuration file.

- If `hazelcast-jet.(yaml|yml|xml)` found on the classpath or at the
  root directory, a member will be created with that configuration file.

- If `hazelcast-client.(yaml|yml|xml)` found on the classpath or at the
  root directory, a client will be created with that configuration file.

- If none of the above conditions are met, a member will be  created
  using the default configuration file (`hazelcast-jet-default.yaml`).

### Health Indicator

Spring-boot has a concept of checking if a provided service is up and
running, this can then be plugged to various monitoring tools. We've
created `HazelcastJetHealthIndicator` which reports the status of the
Jet instance via `HazelcastInstance.getLifecycleService().isRunning()`.

We've also added the auto-configuration of this health indicator
(`HazelcastJetHealthIndicatorAutoConfiguration`) to the
`META-INF/spring.factories`.

## Versioning

We've decided to set the version of the starter as `2.0.0` because it
is compatible with Spring Boot 2.0.0 and above.

## IMDG Conflict

Spring Boot has out of the box support for IMDG starter which creates a
`HazelcastInstance` if `hazelcast.(yaml|yml|xml)` or
`hazelcast-client.(yaml|yml|xml)` found on the classpath or at the root
directory. This means that IMDG starter will create a
`HazelcastInstance` while the Jet starter will create a `JetInstance`.
The solution to this conflict is to disable the IMDG starter if Jet is
present. We've raised a [PR](https://github.com/spring-projects/spring-boot/pull/20729)
to address this on Spring Boot repository.

## Testing

The starter provides unit/integration tests using Spring Boot Test
project.

## Improvements

The health indicator for Jet actually uses the wrapped
`HazelcastInstance` to report the status. We could re-use
`HazelcastHealthIndicator` which is already available in Spring Boot
distribution instead of creating a Jet specific one. The problem is
Spring Boot still uses IMDG `3.12.x` and `com.hazelcast.core.Endpoint`
is moved to `com.hazelcast.cluster.Endpoint` in `4.0`. Since this class
is used in `HazelcastHealthIndicator` we should wait for Spring Boot to
upgrade to IMDG `4.0` to continue this unification. See the
[issue](https://github.com/hazelcast/hazelcast-jet-contrib/issues/63).
