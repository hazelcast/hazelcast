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

import java.io.UncheckedIOException;

import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.newUncheckedIOException;

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
    public static final int SIZEOF_SOCKADDR_IN = 16;
    public static final int SOCKADDR_IN_OFFSETOF_SIN_PORT = 2;
    public static final int SOCKADDR_IN_OFFSETOF_SIN_FAMILY = 0;
    public static final int SOCKADDR_IN_OFFSETOF_SIN_ADDR = 4;
    public static final int IN_ADDRESS_OFFSETOF_S_ADDR = 0;
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

    public static final int EPERM = 1;
    public static final int ENOENT = 2;
    public static final int ESRCH = 3;
    public static final int EINTR = 4;
    public static final int EIO = 5;
    public static final int ENXIO = 6;
    public static final int E2BIG = 7;
    public static final int ENOEXEC = 8;
    public static final int EBADF = 9;
    public static final int ECHILD = 10;
    public static final int EAGAIN = 11;
    public static final int ENOMEM = 12;
    public static final int EACCES = 13;
    public static final int EFAULT = 14;
    public static final int ENOTBLK = 15;
    public static final int EBUSY = 16;
    public static final int EEXIST = 17;
    public static final int EXDEV = 18;
    public static final int ENODEV = 19;
    public static final int ENOTDIR = 20;
    public static final int EISDIR = 21;
    public static final int EINVAL = 22;
    public static final int ENFILE = 23;
    public static final int EMFILE = 24;
    public static final int ENOTTY = 25;
    public static final int ETXTBSY = 26;
    public static final int EFBIG = 27;
    public static final int ENOSPC = 28;
    public static final int ESPIPE = 29;
    public static final int EROFS = 30;
    public static final int EMLINK = 31;
    public static final int EPIPE = 32;
    public static final int EDOM = 33;
    public static final int ERANGE = 34;
    public static final int EDEADLK = 35;
    public static final int ENAMETOOLONG = 36;
    public static final int ENOLCK = 37;
    public static final int ENOSYS = 38;
    public static final int ENOTEMPTY = 39;
    public static final int ELOOP = 40;
    public static final int ENOMSG = 42;
    public static final int EIDRM = 43;
    public static final int ECHRNG = 44;
    public static final int EL2NSYNC = 45;
    public static final int EL3HLT = 46;
    public static final int EL3RST = 47;
    public static final int ELNRNG = 48;
    public static final int EUNATCH = 49;
    public static final int ENOCSI = 50;
    public static final int EL2HLT = 51;
    public static final int EBADE = 52;
    public static final int EBADR = 53;
    public static final int EXFULL = 54;
    public static final int ENOANO = 55;
    public static final int EBADRQC = 56;
    public static final int EBADSLT = 57;
    public static final int EBFONT = 59;
    public static final int ENOSTR = 60;
    public static final int ENODATA = 61;
    public static final int ETIME = 62;
    public static final int ENOSR = 63;
    public static final int ENONET = 64;
    public static final int ENOPKG = 65;
    public static final int EREMOTE = 66;
    public static final int ENOLINK = 67;
    public static final int EADV = 68;
    public static final int ESRMNT = 69;
    public static final int ECOMM = 70;
    public static final int EPROTO = 71;
    public static final int EMULTIHOP = 72;
    public static final int EDOTDOT = 73;
    public static final int EBADMSG = 74;
    public static final int EOVERFLOW = 75;
    public static final int ENOTUNIQ = 76;
    public static final int EBADFD = 77;
    public static final int EREMCHG = 78;
    public static final int ELIBACC = 79;
    public static final int ELIBBAD = 80;
    public static final int ELIBSCN = 81;
    public static final int ELIBMAX = 82;
    public static final int ELIBEXEC = 83;
    public static final int EILSEQ = 84;
    public static final int ERESTART = 85;
    public static final int ESTRPIPE = 86;
    public static final int EUSERS = 87;
    public static final int ENOTSOCK = 88;
    public static final int EDESTADDRREQ = 89;
    public static final int EMSGSIZE = 90;
    public static final int EPROTOTYPE = 91;
    public static final int ENOPROTOOPT = 92;
    public static final int EPROTONOSUPPORT = 93;
    public static final int ESOCKTNOSUPPORT = 94;
    public static final int EOPNOTSUPP = 95;
    public static final int EPFNOSUPPORT = 96;
    public static final int EAFNOSUPPORT = 97;
    public static final int EADDRINUSE = 98;
    public static final int EADDRNOTAVAIL = 99;
    public static final int ENETDOWN = 100;
    public static final int ENETUNREACH = 101;
    public static final int ENETRESET = 102;
    public static final int ECONNABORTED = 103;
    public static final int ECONNRESET = 104;
    public static final int ENOBUFS = 105;
    public static final int EISCONN = 106;
    public static final int ENOTCONN = 107;
    public static final int ESHUTDOWN = 108;
    public static final int ETOOMANYREFS = 109;
    public static final int ETIMEDOUT = 110;
    public static final int ECONNREFUSED = 111;
    public static final int EHOSTDOWN = 112;
    public static final int EHOSTUNREACH = 113;
    public static final int EALREADY = 114;
    public static final int EINPROGRESS = 115;
    public static final int ESTALE = 116;
    public static final int EUCLEAN = 117;
    public static final int ENOTNAM = 118;
    public static final int ENAVAIL = 119;
    public static final int EISNAM = 120;
    public static final int EREMOTEIO = 121;
    public static final int EDQUOT = 122;
    public static final int ENOMEDIUM = 123;
    public static final int EMEDIUMTYPE = 124;
    public static final int ECANCELED = 125;
    public static final int ENOKEY = 126;
    public static final int EKEYEXPIRED = 127;
    public static final int EKEYREVOKED = 128;
    public static final int EKEYREJECTED = 129;
    public static final int EOWNERDEAD = 130;
    public static final int ENOTRECOVERABLE = 131;
    public static final int ERFKILL = 132;
    public static final int EHWPOISON = 133;


    static {
        UringLibrary.ensureAvailable();
    }

    private Linux() {
    }

    public static UncheckedIOException newSysCallFailedException(String msg, String syscall, int errnum) {
        return newUncheckedIOException(msg + " "
                + syscall + " failed with error " + errorcode(errnum) + " '" + strerror(errnum) + "'. "
                + "Check " + toManPagesUrl(syscall) + " for more detail.");
    }

    public static String toManPagesUrl(String syscall) {
        String s = syscall.replace("(", ".").replace(")", ".");
        String man = syscall.endsWith("p)") ? "man3" : "man2";
        return "https://man7.org/linux/man-pages/" + man + "/" + s + "html";
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
            case EPERM:
                return "EPERM";
            case ENOENT:
                return "ENOENT";
            case ESRCH:
                return "ESRCH";
            case EINTR:
                return "EINTR";
            case EIO:
                return "EIO";
            case ENXIO:
                return "ENXIO";
            case E2BIG:
                return "E2BIG";
            case ENOEXEC:
                return "ENOEXEC";
            case EBADF:
                return "EBADF";
            case ECHILD:
                return "ECHILD";
            case EAGAIN:
                return "EAGAIN";
            case ENOMEM:
                return "ENOMEM";
            case EACCES:
                return "EACCES";
            case EFAULT:
                return "EFAULT";
            case ENOTBLK:
                return "ENOTBLK";
            case EBUSY:
                return "EBUSY";
            case EEXIST:
                return "EEXIST";
            case EXDEV:
                return "EXDEV";
            case ENODEV:
                return "ENODEV";
            case ENOTDIR:
                return "ENOTDIR";
            case EISDIR:
                return "EISDIR";
            case EINVAL:
                return "EINVAL";
            case ENFILE:
                return "ENFILE";
            case EMFILE:
                return "EMFILE";
            case ENOTTY:
                return "ENOTTY";
            case ETXTBSY:
                return "ETXTBSY";
            case EFBIG:
                return "EFBIG";
            case ENOSPC:
                return "ENOSPC";
            case ESPIPE:
                return "ESPIPE";
            case EROFS:
                return "EROFS";
            case EMLINK:
                return "EMLINK";
            case EPIPE:
                return "EPIPE";
            case EDOM:
                return "EDOM";
            case ERANGE:
                return "ERANGE";
            case EDEADLK:
                return "EDEADLK";
            case ENAMETOOLONG:
                return "ENAMETOOLONG";
            case ENOLCK:
                return "ENOLCK";
            case ENOSYS:
                return "ENOSYS";
            case ENOTEMPTY:
                return "ENOTEMPTY";
            case ELOOP:
                return "ELOOP";
            case ENOMSG:
                return "ENOMSG";
            case EIDRM:
                return "EIDRM";
            case ECHRNG:
                return "ECHRNG";
            case EL2NSYNC:
                return "EL2NSYNC";
            case EL3HLT:
                return "EL3HLT";
            case EL3RST:
                return "EL3RST";
            case ELNRNG:
                return "ELNRNG";
            case EUNATCH:
                return "EUNATCH";
            case ENOCSI:
                return "ENOCSI";
            case EL2HLT:
                return "EL2HLT";
            case EBADE:
                return "EBADE";
            case EBADR:
                return "EBADR";
            case EXFULL:
                return "EXFULL";
            case ENOANO:
                return "ENOANO";
            case EBADRQC:
                return "EBADRQC";
            case EBADSLT:
                return "EBADSLT";
            case EBFONT:
                return "EBFONT";
            case ENOSTR:
                return "ENOSTR";
            case ENODATA:
                return "ENODATA";
            case ETIME:
                return "ETIME";
            case ENOSR:
                return "ENOSR";
            case ENONET:
                return "ENONET";
            case ENOPKG:
                return "ENOPKG";
            case EREMOTE:
                return "EREMOTE";
            case ENOLINK:
                return "ENOLINK";
            case EADV:
                return "EADV";
            case ESRMNT:
                return "ESRMNT";
            case ECOMM:
                return "ECOMM";
            case EPROTO:
                return "EPROTO";
            case EMULTIHOP:
                return "EMULTIHOP";
            case EDOTDOT:
                return "EDOTDOT";
            case EBADMSG:
                return "EBADMSG";
            case EOVERFLOW:
                return "EOVERFLOW";
            case ENOTUNIQ:
                return "ENOTUNIQ";
            case EBADFD:
                return "EBADFD";
            case EREMCHG:
                return "EREMCHG";
            case ELIBACC:
                return "ELIBACC";
            case ELIBBAD:
                return "ELIBBAD";
            case ELIBSCN:
                return "ELIBSCN";
            case ELIBMAX:
                return "ELIBMAX";
            case ELIBEXEC:
                return "ELIBEXEC";
            case EILSEQ:
                return "EILSEQ";
            case ERESTART:
                return "ERESTART";
            case ESTRPIPE:
                return "ESTRPIPE";
            case EUSERS:
                return "EUSERS";
            case ENOTSOCK:
                return "ENOTSOCK";
            case EDESTADDRREQ:
                return "EDESTADDRREQ";
            case EMSGSIZE:
                return "EMSGSIZE";
            case EPROTOTYPE:
                return "EPROTOTYPE";
            case ENOPROTOOPT:
                return "ENOPROTOOPT";
            case EPROTONOSUPPORT:
                return "EPROTONOSUPPORT";
            case ESOCKTNOSUPPORT:
                return "ESOCKTNOSUPPORT";
            case EOPNOTSUPP:
                return "EOPNOTSUPP";
            case EPFNOSUPPORT:
                return "EPFNOSUPPORT";
            case EAFNOSUPPORT:
                return "EAFNOSUPPORT";
            case EADDRINUSE:
                return "EADDRINUSE";
            case EADDRNOTAVAIL:
                return "EADDRNOTAVAIL";
            case ENETDOWN:
                return "ENETDOWN";
            case ENETUNREACH:
                return "ENETUNREACH";
            case ENETRESET:
                return "ENETRESET";
            case ECONNABORTED:
                return "ECONNABORTED";
            case ECONNRESET:
                return "ECONNRESET";
            case ENOBUFS:
                return "ENOBUFS";
            case EISCONN:
                return "EISCONN";
            case ENOTCONN:
                return "ENOTCONN";
            case ESHUTDOWN:
                return "ESHUTDOWN";
            case ETOOMANYREFS:
                return "ETOOMANYREFS";
            case ETIMEDOUT:
                return "ETIMEDOUT";
            case ECONNREFUSED:
                return "ECONNREFUSED";
            case EHOSTDOWN:
                return "EHOSTDOWN";
            case EHOSTUNREACH:
                return "EHOSTUNREACH";
            case EALREADY:
                return "EALREADY";
            case EINPROGRESS:
                return "EINPROGRESS";
            case ESTALE:
                return "ESTALE";
            case EUCLEAN:
                return "EUCLEAN";
            case ENOTNAM:
                return "ENOTNAM";
            case ENAVAIL:
                return "ENAVAIL";
            case EISNAM:
                return "EISNAM";
            case EREMOTEIO:
                return "EREMOTEIO";
            case EDQUOT:
                return "EDQUOT";
            case ENOMEDIUM:
                return "ENOMEDIUM";
            case EMEDIUMTYPE:
                return "EMEDIUMTYPE";
            case ECANCELED:
                return "ECANCELED";
            case ENOKEY:
                return "ENOKEY";
            case EKEYEXPIRED:
                return "EKEYEXPIRED";
            case EKEYREVOKED:
                return "EKEYREVOKED";
            case EKEYREJECTED:
                return "EKEYREJECTED";
            case EOWNERDEAD:
                return "EOWNERDEAD";
            case ENOTRECOVERABLE:
                return "ENOTRECOVERABLE";
            case ERFKILL:
                return "ERFKILL";
            case EHWPOISON:
                return "EHWPOISON";
            default:
                return "unknown-code[" + errnum + "]";
        }
    }

    public static native void remove(String pathname);

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

    public static native long filesize(int fd);

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
