# Dynamic Log Level

### Background
#### Description

Support dynamic log level changing without restarting a member.

#### Motivation

Occasionally, users want to make the logging more verbose for a short period of
time without restarting members. For instance, that is useful while diagnosing
some issue.

#### Goals

- Provide an internal API for dynamic log level adjustment.
- Make the API accessible through REST.
- Make the API accessible through JMX.

#### Non-Goals

- Providing public API.
- Per logger granularity of the log level adjustment.
- Providing support for MC.
- Providing support for CLI.

#### Actors and Scenarios

1. User wants to obtain a more detailed log on a certain member.
2. User makes the log level more verbose on the member.
3. User reproduces an issue in question.
4. User resets the log level back.
5. User collects the log.
6. User inspects the log or sends it to support.

### Technical Design

#### Internal API

Hazelcast supports multiple logger frameworks out of the box. The difference
between logging backends is abstracted away by `LoggerFactory` and `ILogger`
interfaces which belong to the public API. Since we don't want to provide any
public API for the log level adjustment, internal counterparts should be
introduced: `InternalLoggerFactory` and `InternalLogger`:

```java
public interface InternalLoggerFactory {

    /**
     * Sets the levels of all the loggers known to this logger factory to
     * the given level. If a certain logger was already preconfigured with a more
     * verbose level than the given level, it will be kept at that more verbose
     * level.
     *
     * @param level the level to set.
     */
    void setLevel(@Nonnull Level level);

    /**
     * Resets the levels of all the loggers known to this logger factory back
     * to the default preconfigured values. Basically, undoes all the changes
     * done by the previous calls to {@link #setLevel}, if there were any.
     */
    void resetLevel();

}
```

```java
public interface InternalLogger {

    /**
     * Sets the level of this logger to the given level.
     *
     * @param level the level to set, can be {@code null} if the underlying
     *              logging framework gives some special meaning to it (like
     *              inheriting the log level from some parent object).
     */
    void setLevel(@Nullable Level level);

}
```

For each logging backend Hazelcast provides a `LoggerFactory` implementation
which in turn provides an `ILogger` implementation. To support dynamic log level
changing the implementations should additionally implement the internal
counterpart (`InternalLoggerFactory` and `InternalLogger`). The following
backends support the dynamic log level changing currently:

- JDK/JUL (default)
- log4j
- log4j2

The only unsupported backend is slf4j since it doesn't provide any public or
internal API for level changing. For each logging backend supported by slf4j a
separate specialized code should be written to extract the loggers from internal
slf4j wrappers. The problem is that the wrappers are internal and specific for
every logging backend, so they are not necessarily stable across versions of
slf4j. In theory, we could support some set of popular logging backends for
slf4j, but that would require inspecting at least recent versions of slf4j and
wrappers to understand how stable the implementation of each wrapper is and how
we could access it, regularly or through reflection. For that reasons slf4j
support was postponed.

`LoggingServiceImpl` should provide methods for getting, setting and resetting
the log level of the `loggerFactory` managed by it:

```java
/**
  * @return the log level of this logging service previously set by {@link
  * #setLevel}, or {@code null} if no level was set or it was reset by {@link
  * #resetLevel}.
  */
public @Nullable Level getLevel();


/**
  * Sets the levels of all the loggers known to this logger service to
  * the given level. If a certain logger was already preconfigured with a more
  * verbose level than the given level, it will be kept at that more verbose
  * level.
  * <p>
  * WARNING: Keep in mind that verbose log levels like {@link Level#FINEST}
  * may severely affect the performance.
  *
  * @param level the level to set.
  * @throws HazelcastException if the underlying {@link LoggerFactory} doesn't
  *                            implement {@link InternalLoggerFactory} required
  *                            for dynamic log level changing.
  */
public void setLevel(@Nonnull Level level);

  /**
   * Parses the given string level into {@link Level} and then sets the level
   * using {@link #setLevel(Level)}.
   *
   * @param level the level to parse, see {@link Level#getName()} for available
   *              level names.
   * @throws IllegalArgumentException if the passed string can't be parsed into
   *                                  a known {@link Level}.
   */
  public void setLevel(@Nonnull String level);

/**
  * Resets the levels of all the loggers known to this logging service back
  * to the default reconfigured values. Basically, undoes all the changes done
  * by the previous calls to {@link #setLevel}, if there were any.
  */
public void resetLevel();
```

Log level changes performed by `LoggingServiceImpl` should be reported to audit
log.

#### REST endpoint

REST endpoint at `/hazelcast/rest/log-level` should be exposed to users; users
should be able to: GET it to learn the current log level set, POST to it to set
the log level to the value they want to. REST endpoint at
`/hazelcast/rest/log-level/reset` should be exposed to users; users should be
able to POST to it to reset the log level back.

The endpoints should have a proper `RestEndpointGroup` assigned to control the
access, mutating operations should be password protected.

#### JMX

Logging service should expose its JMX MBean (`LoggingServiceMBean`) to users.
User should be able to get level, set the level and reset the level.

### Testing Criteria

- Cover every logging backend with a separate test to make sure the applied log
  level is actually taking effect on the logging backend.
- Provide functional tests for REST and JMX.
