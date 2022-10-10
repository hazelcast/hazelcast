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

public final class Linux {

    //todo: should not be hardcoded value.
    public final static int IOV_MAX = 1024;

    public static final int PERMISSIONS_ALL = 0666;

    public static final int SIZEOF_KERNEL_TIMESPEC = 16;
    public static final int SIZEOF_SOCKADDR_STORAGE = 128;
    // assuming 64 bit Linux
    public final static int SIZEOF_IOVEC = 16;

    public static final int FALSE = 0;

    public static final int O_RDONLY = 00;
    public static final int O_WRONLY = 01;
    public static final int O_RDWR = 02;
    public static final int O_CREAT = 0100;
    public static final int O_TRUNC = 01000;
    public static final int O_DIRECT = 040000;
    public static final int O_SYNC = 04000000;

    public static final int SOCK_CLOEXEC = 02000000;
    public static final int SOCK_NONBLOCK = 04000;

    public static final int SO_DEBUG = 1;
    public static final int SO_REUSEADDR = 2;
    public static final int SO_TYPE = 3;
    public static final int SO_ERROR = 4;
    public static final int SO_DONTROUTE = 5;
    public static final int SO_BROADCAST = 6;
    public static final int SO_SNDBUF = 7;
    public static final int SO_RCVBUF = 8;
    public static final int SO_KEEPALIVE = 9;
    public static final int SO_OOBINLINE = 10;
    public static final int SO_NO_CHECK = 11;
    public static final int SO_PRIORITY = 12;
    public static final int SO_LINGER = 13;
    public static final int SO_BSDCOMPAT = 14;
    public static final int SO_REUSEPORT = 15;

    static {
        IOUringLibrary.ensureAvailable();
    }

    private Linux() {
    }

    public static native int errno();

    /**
     * Returns the error message for a given errnum.
     * <p>
     * https://man7.org/linux/man-pages/man3/strerror.3.html
     *
     * @param errnum the error number.
     * @return the error message.
     */
    public static native String strerror(int errnum);

    /**
     * https://man7.org/linux/man-pages/man2/eventfd.2.html
     *
     * @param initval
     * @param flags
     * @return
     */
    public static native int eventfd(int initval, int flags);

    /**
     * https://man7.org/linux/man-pages/man2/open.2.html
     *
     * @param pathname
     * @param flags
     * @param mode
     * @return
     */
    public static native int open(String pathname, long flags, int mode);

    /**
     * https://man7.org/linux/man-pages/man2/close.2.html
     *
     * @param fd
     * @return
     */
    public static native int close(int fd);

    /**
     * https://man7.org/linux/man-pages/man2/read.2.html
     *
     * @param fd
     * @param buf
     * @param count
     * @return
     */
    public static native long read(int fd, long buf, long count);

    /**
     * https://man7.org/linux/man-pages/man2/write.2.html
     *
     * @param fd
     * @param buf
     * @param count
     * @return
     */
    public static native long write(int fd, long buf, long count);

    public static native int eventfd_write(int fd, long value);

    /**
     * https://man7.org/linux/man-pages/man2/pread.2.html
     *
     * @param fd
     * @param buf
     * @param count
     * @param offset
     * @return
     */
    public static native long pread(int fd, long buf, long count, long offset);

    /**
     * https://man7.org/linux/man-pages/man3/pwrite.3p.html
     *
     * @param fd
     * @param buf
     * @param count
     * @param offset
     * @return
     */
    public static native long pwrite(int fd, long buf, long count, long offset);

    /**
     * https://man7.org/linux/man-pages/man1/sync.1.html
     */
    public static native void sync();

    /**
     * https://linux.die.net/man/2/syncfs
     *
     * @param fd
     * @return
     */
    public static native int syncfs(int fd);

    /**
     * https://man7.org/linux/man-pages/man2/fsync.2.html
     *
     * @param fd
     * @return
     */
    public static native int fsync(int fd);

    /**
     * https://linux.die.net/man/2/fdatasync
     *
     * @param fd
     * @return
     */
    public static native int fdatasync(int fd);
//
//    public static native int getsockopt(int sockfd, int level, int optname,
//                   long restrict optval, int restrict optlen);
//    public static native  int setsockopt(int sockfd, int level, int optname,
//                      const void *optval, socklen_t optlen);

    public static native int clock_gettime(int clk_id, long tp);

    /**
     * Returns the CLOCK_REALTIME time in nanoseconds.
     * <p/>
     * https://linux.die.net/man/3/clock_gettime
     *
     * @return time in nanoseconds
     */
    public static native long clock_currentTime();

    /**
     * Returns the CLOCK_MONOTONIC time in nanoseconds.
     * <p/>
     * https://linux.die.net/man/3/clock_gettime
     *
     * @return time in nanoseconds
     */
    public static native long clock_monotonicTime();

    /**
     * Returns the CLOCK_PROCESS_CPUTIME_ID time in nanoseconds.
     * <p/>
     * https://linux.die.net/man/3/clock_gettime
     *
     * @return time in nanoseconds
     */
    public static native long clock_processCpuTime();

    /**
     * Returns the CLOCK_THREAD_CPUTIME_ID time in nanoseconds.
     * <p/>
     * https://linux.die.net/man/3/clock_gettime
     *
     * @return time in nanoseconds
     */
    public static native long clock_threadCpuTime();
}
