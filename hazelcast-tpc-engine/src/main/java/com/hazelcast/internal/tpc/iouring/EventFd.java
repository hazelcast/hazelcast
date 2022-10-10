/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.tpc.iouring;

import java.io.IOException;
import java.io.UncheckedIOException;

import static com.hazelcast.internal.tpc.iouring.Linux.strerror;

/**
 * Represents an event-filedescriptor. One of the applications is the
 * notification of a thread that is blocking on the io_uring_enter.
 */
public final class EventFd implements AutoCloseable {

    private final int fd;
    private boolean closed;

    public EventFd() {
        int res = Linux.eventfd(0, 0);
        if (res == -1) {
            throw new UncheckedIOException(new IOException("Failed to create eventfd " + strerror(Linux.errno())));
        }
        this.fd = res;
    }

    public void write(long value) {
        int res = Linux.eventfd_write(fd, value);
        if (res == -1) {
            throw new UncheckedIOException(new IOException("Failed to write to eventfd " + strerror(Linux.errno())));
        }
    }

    public int fd() {
        return fd;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        closed = true;
        Linux.close(fd);
    }
}
