
## SlowOperationDetector

The `SlowOperationDetector` monitors the operation threads and collects information about all slow operations. An `Operation` is a task executed by a generic or partition thread (see [Operation Threading](#operation-threading)). An operation is considered as slow when it takes more computation time than the configured threshold.

The `SlowOperationDetector` stores the fully qualified classname of the operation and its stacktrace as well as operation details, start time and duration of each slow invocation. All collected data is available in the [Management Center](#members).

The `SlowOperationDetector` is configured via system properties. Please refer to the [System Properties section](#system-properties) for details.

### Logging of Slow Operations

The detected slow operations are logged as warning in the Hazelcast log files:
```
WARN 2015-05-07 11:05:30,890 SlowOperationDetector: [127.0.0.1]:5701
  Slow operation detected: com.hazelcast.map.impl.operation.PutOperation
  Hint: You can enable the logging of stacktraces with the following config
  property: hazelcast.slow.operation.detector.stacktrace.logging.enabled
WARN 2015-05-07 11:05:30,891 SlowOperationDetector: [127.0.0.1]:5701
  Slow operation detected: com.hazelcast.map.impl.operation.PutOperation
  (2 invocations)
WARN 2015-05-07 11:05:30,892 SlowOperationDetector: [127.0.0.1]:5701
  Slow operation detected: com.hazelcast.map.impl.operation.PutOperation
  (3 invocations)
```

Stacktraces will always be reported to the Management Center, but by default they are not printed to keep the log size small. If logging of stacktraces is enabled the full stacktrace is printed every 100 invocations. All other invocations print a shortened version.

### Purging of Slow Operation Logs

Since a Hazelcast cluster can run for a very long time we purge the slow operation logs periodically to prevent an OOME. You can configure the purge interval and the retention time for each invocation.

The purging removes each invocation whose retention time is exceeded. When all invocations are purged from a slow operation log, the log itself will be deleted.
