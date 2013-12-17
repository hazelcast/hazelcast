package com.hazelcast.spi;

/**
 * An Marker interface that signals that an operation is a System Operation.
 *
 * System Operations can be executed by the {@link com.hazelcast.spi.OperationService} with a higher priority. This
 * is important because when a system is under load and its operation queues are filled up, you want the system to
 * deal with system operation like the ones needed for partition-migration, with a high priority. So that system
 * remains responsive.
 *
 * In most cases this interface should not be used by normal user code because illegal usage of this interface,
 * can influence the health of the Hazelcast cluster negatively.
 */
public interface SystemOperation {
}
