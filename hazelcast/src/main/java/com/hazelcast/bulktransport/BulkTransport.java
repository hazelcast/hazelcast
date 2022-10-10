package com.hazelcast.bulktransport;

import java.io.File;

/**
 * Functionality for bulk transport. Dedicated TCP/IP connections are used.
 *
 * Implementations are not thread safe.
 */
public interface BulkTransport {

    void copyFile(File file);

    void copyMemory(long address, long length);

    void copyFake(long length);

    void close();
}
