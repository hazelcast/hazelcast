/**
 * Contains functionality for the SlowOperationDetector.
 *
 * The SlowOperationDetector scans the generic and partition {@link com.hazelcast.spi.impl.operationexecutor.OperationRunner}
 * instances and detects which operations have been running for a too long period.
 */
package com.hazelcast.spi.impl.slowoperationdetector;
