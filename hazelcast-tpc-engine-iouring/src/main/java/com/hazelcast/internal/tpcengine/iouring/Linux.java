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

@SuppressWarnings({"checkstyle:MagicNumber",
        "checkstyle:ConstantName",
        "checkstyle:MethodName",
        "checkstyle:ParameterName",
        "checkstyle:IllegalTokenText"})
public final class Linux {

    //todo: should not be hardcoded value.
    public static final int IOV_MAX = 1024;

    public static final int PERMISSIONS_ALL = 0666;

    public static final int SIZEOF_KERNEL_TIMESPEC = 16;
    public static final int SIZEOF_SOCKADDR_STORAGE = 128;
    // assuming 64 bit Linux
    public static final int SIZEOF_IOVEC = 16;

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

    /**
     * Returns the error code for a given error number. This makes lookup up the cause
     * of a Linux failure a lot easier since you don't need to deal with anumber like '19'
     * but get a code like 'ENODEV'.
     *
     * @param errnum the error number.
     * @return the error code. For an unknown error number, 'unknown-code[errornumber]'
     * is returned. So this method will never throw an Exception.
     */
    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:Returncount", "checkstyle:CyclomaticComplexity"})
    public static String errorcode(int errnum) {
        switch (errnum) {
            case 1:
                return "EPERM";
            case 2:
                return "ENOENT";
            case 3:
                return "ESRCH";
            case 4:
                return "EINTR";
            case 5:
                return "EIO";
            case 6:
                return "ENXIO";
            case 7:
                return "E2BIG";
            case 8:
                return "ENOEXEC";
            case 9:
                return "EBADF";
            case 10:
                return "ECHILD";
            case 11:
                return "EAGAIN";
            case 12:
                return "ENOMEM";
            case 13:
                return "EACCES";
            case 14:
                return "EFAULT";
            case 15:
                return "ENOTBLK";
            case 16:
                return "EBUSY";
            case 17:
                return "EEXIST";
            case 18:
                return "EXDEV";
            case 19:
                return "ENODEV";
            case 20:
                return "ENOTDIR";
            case 21:
                return "EISDIR";
            case 22:
                return "EINVAL";
            case 23:
                return "ENFILE";
            case 24:
                return "EMFILE";
            case 25:
                return "ENOTTY";
            case 26:
                return "ETXTBSY";
            case 27:
                return "ENOSPC";
            case 28:
                return "ESPIPE";
            case 29:
                return "EROFS";
            case 30:
                return "EMLINK";
            case 31:
                return "EPIPE";
            case 32:
                return "EDOM";
            case 33:
                return "ESRCH";
            case 34:
                return "ERANGE";
            case 35:
                return "EDEADLK";
            case 36:
                return "ENAMETOOLONG";
            case 37:
                return "ENOLCK";
            case 38:
                return "ENOSYS";
            case 39:
                return "ENOTEMPTY";
            case 40:
                return "ELOOP";
            case 42:
                return "ENOMSG";
            case 43:
                return "EIDRM";
            case 44:
                return "ECHRNG";
            case 45:
                return "EL2NSYNC";
            case 46:
                return "EL3HLT";
            case 47:
                return "EL3RST";
            case 48:
                return "ELNRNG";
            case 49:
                return "EUNATCH";
            case 50:
                return "ENOCSI";
            case 51:
                return "EL2HLT";
            case 52:
                return "EBADE";
            case 53:
                return "EBADR";
            case 54:
                return "EXFULL";
            case 55:
                return "ENOANO";
            case 56:
                return "EBADRQC";
            case 57:
                return "EBADSLT";
            case 59:
                return "EBFONT";
            case 60:
                return "ENOSTR";
            case 61:
                return "ENODATA";
            case 62:
                return "ETIME";
            case 63:
                return "ENOSR";
            case 64:
                return "ENONET";
            case 65:
                return "ENOPKG";
            case 66:
                return "EREMOTE";
            case 67:
                return "ENOLINK";
            case 68:
                return "EADV";
            case 69:
                return "ESRMNT";
            case 70:
                return "ECOMM";
            case 71:
                return "EPROTO";
            case 72:
                return "EMULTIHOP";
            case 73:
                return "EDOTDOT";
            case 74:
                return "EBADMSG";
            case 75:
                return "EOVERFLOW";
            case 76:
                return "ENOTUNIQ";
            case 77:
                return "EBADFD";
            case 78:
                return "EREMCHG";
            case 79:
                return "ELIBACC";
            case 80:
                return "ELIBBAD";
            case 81:
                return "ELIBSCN";
            case 82:
                return "ELIBMAX";
            case 83:
                return "ELIBEXEC";
            case 84:
                return "EILSEQ";
            case 85:
                return "ERESTART";
            case 86:
                return "ESTRPIPE";
            case 87:
                return "EUSERS";
            case 88:
                return "ENOTSOCK";
            case 89:
                return "EDESTADDRREQ";
            case 90:
                return "EMSGSIZE";
            case 91:
                return "EPROTOTYPE";
            case 92:
                return "ENOPROTOOPT";
            case 93:
                return "EPROTONOSUPPORT";
            case 94:
                return "ESOCKTNOSUPPORT";
            case 95:
                return "EOPNOTSUPP";
            case 96:
                return "EPFNOSUPPORT";
            case 97:
                return "EAFNOSUPPORT";
            case 98:
                return "EADDRINUSE";
            case 99:
                return "EADDRNOTAVAIL";
            case 100:
                return "ENETDOWN";
            case 101:
                return "ENETUNREACH";
            case 102:
                return "ENETRESET";
            case 103:
                return "ECONNABORTED";
            case 104:
                return "ECONNRESET";
            case 105:
                return "ENOBUFS";
            case 106:
                return "EISCONN";
            case 107:
                return "ENOTCONN";
            case 108:
                return "ESHUTDOWN";
            case 109:
                return "ETOOMANYREFS";
            case 110:
                return "ETIMEDOUT";
            case 111:
                return "ECONNREFUSED";
            case 112:
                return "EHOSTDOWN";
            case 113:
                return "EHOSTUNREACH";
            case 114:
                return "EALREADY";
            case 115:
                return "EINPROGRESS";
            case 116:
                return "ESTALE";
            case 117:
                return "EUCLEAN";
            case 118:
                return "ENOTNAM";
            case 119:
                return "ENAVAIL";
            case 120:
                return "EISNAM";
            case 121:
                return "EREMOTEIO";
            case 122:
                return "EDQUOT";
            case 123:
                return "ENOMEDIUM";
            case 124:
                return "EMEDIUMTYPE";
            case 125:
                return "ECANCELED";
            case 126:
                return "ENOKEY";
            case 127:
                return "EKEYEXPIRED";
            case 128:
                return "EKEYREVOKED";
            case 129:
                return "EKEYREJECTED";
            case 130:
                return "EOWNERDEAD";
            case 131:
                return "ENOTRECOVERABLE";
            default:
                return "unknown-code[" + errnum + "]";
        }
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
