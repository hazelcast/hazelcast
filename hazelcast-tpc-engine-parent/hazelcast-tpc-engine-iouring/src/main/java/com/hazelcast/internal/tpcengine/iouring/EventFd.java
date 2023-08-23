/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.iouring;

import static com.hazelcast.internal.tpcengine.iouring.Linux.newSysCallFailedException;

/**
 * Represents an event file-descriptor. One of the applications is the
 * notification of a thread that is blocking on the io_uring_enter.
 * <p/>
 * EventFd is not threadsafe.
 */
public final class EventFd implements AutoCloseable {

    private final int fd;
    private boolean closed;

    public EventFd() {
        int res = Linux.eventfd(0, 0);
        if (res == -1) {
            throw newSysCallFailedException("Failed to create eventfd.", "eventfd(2)", -res);
        }
        this.fd = res;
    }

    /**
     * Writes a value to this EventFd.
     * <p/>
     * This will trigger a wakeup to any call waiting for a write on this EventFd.
     * <p/>
     * See {@link https://linux.die.net/man/3/eventfd_write} for more info.
     *
     * @param value a value.
     * @return the res. Use the {@link Linux#errno()} to get the error number.
     */
    public int write(long value) {
        return Linux.eventfd_write(fd, value);
    }

    /**
     * Returns the file descriptor.
     *
     * @return the file descriptor.
     */
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
