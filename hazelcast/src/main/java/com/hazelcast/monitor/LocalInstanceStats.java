package com.hazelcast.monitor;

public interface LocalInstanceStats<T extends LocalInstanceOperationStats> {

	/**
	 * Returns the operation stats for this member.
	 * 
	 * @return operation stats
	 */
	T getOperationStats();
}
