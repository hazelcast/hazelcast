package com.hazelcast.internal.tpc;

/**
 * Represents some storage device.
 *
 * One of the problems is that every eventloop will assume full ownership of this device.
 */
public class StorageDevice {
    public final int maxConcurrent;
    public final String path;
    public final int maxPending;

    public StorageDevice(String path,
                         int maxConcurrent,
                         int maxPending) {
        this.path = path;
        this.maxConcurrent = maxConcurrent;
        this.maxPending = maxPending;
    }
}
