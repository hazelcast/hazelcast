package com.hazelcast.monitor;

import com.hazelcast.nio.DataSerializable;

public interface LocalInstanceOperationStats extends DataSerializable {

	/**
     * Gets the start time of the period in milliseconds.
     *
     * @return start time in milliseconds.
     */
    public long getPeriodStart();

    /**
     * Gets the end time of the period in milliseconds.
     *
     * @return end time in milliseconds.
     */
    public long getPeriodEnd();
}
