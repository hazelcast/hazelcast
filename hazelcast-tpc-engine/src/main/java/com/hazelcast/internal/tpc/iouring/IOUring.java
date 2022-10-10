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

import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpc.util.Preconditions.checkPositive;

/**
 * This class should get all the JNI methods.
 * <p>
 * Good read:
 * https://github.com/axboe/liburing/issues/536
 * https://tchaloupka.github.io/during/during.io_uring.RegisterOpCode.html
 */
public final class IOUring implements AutoCloseable {

    static {
        IOUringLibrary.ensureAvailable();
    }

    public final static byte IORING_OP_NOP = 0;
    public final static byte IORING_OP_READV = 1;
    public final static byte IORING_OP_WRITEV = 2;
    public final static byte IORING_OP_FSYNC = 3;
    public final static byte IORING_OP_READ_FIXED = 4;
    public final static byte IORING_OP_WRITE_FIXED = 5;
    public final static byte IORING_OP_POLL_ADD = 6;
    public final static byte IORING_OP_POLL_REMOVE = 7;
    public final static byte IORING_OP_SYNC_FILE_RANGE = 8;
    public final static byte IORING_OP_SENDMSG = 9;
    public final static byte IORING_OP_RECVMSG = 10;
    public final static byte IORING_OP_TIMEOUT = 11;
    public final static byte IORING_OP_TIMEOUT_REMOVE = 12;
    public final static byte IORING_OP_ACCEPT = 13;
    public final static byte IORING_OP_ASYNC_CANCEL = 14;
    public final static byte IORING_OP_LINK_TIMEOUT = 15;
    public final static byte IORING_OP_CONNECT = 16;
    public final static byte IORING_OP_FALLOCATE = 17;
    public final static byte IORING_OP_OPENAT = 18;
    public final static byte IORING_OP_CLOSE = 19;
    public final static byte IORING_OP_FILES_UPDATE = 20;
    public final static byte IORING_OP_STATX = 21;
    public final static byte IORING_OP_READ = 22;
    public final static byte IORING_OP_WRITE = 23;
    public final static byte IORING_OP_FADVISE = 24;
    public final static byte IORING_OP_MADVISE = 25;
    public final static byte IORING_OP_SEND = 26;
    public final static byte IORING_OP_RECV = 27;
    public final static byte IORING_OP_OPENAT2 = 28;
    public final static byte IORING_OP_EPOLL_CTL = 29;
    public final static byte IORING_OP_SPLICE = 30;
    public final static byte IORING_OP_PROVIDE_BUFFERS = 31;
    public final static byte IORING_OP_REMOVE_BUFFERS = 32;
    public final static byte IORING_OP_TEE = 33;
    public final static byte IORING_OP_SHUTDOWN = 34;
    public final static byte IORING_OP_RENAMEAT = 35;
    public final static byte IORING_OP_UNLINKAT = 36;
    public final static byte IORING_OP_MKDIRAT = 37;
    public final static byte IORING_OP_SYMLINKAT = 38;
    public final static byte IORING_OP_LINKAT = 39;
    public final static byte IORING_OP_MSG_RING = 40;
    public final static byte IORING_OP_FSETXATTR = 41;
    public final static byte IORING_OP_SETXATTR = 42;
    public final static byte IORING_OP_FGETXATTR = 43;
    public final static byte IORING_OP_GETXATTR = 44;
    public final static byte IORING_OP_SOCKET = 45;
    public final static byte IORING_OP_URING_CMD = 46;
    public final static byte IORING_OP_SEND_ZC = 47;
    public final static byte IORING_OP_SENDMSG_ZC = 48;

    public static final int IORING_SETUP_IOPOLL = 1 << 0;
    public static final int IORING_SETUP_SQPOLL = 1 << 1;
    public static final int IORING_SETUP_SQ_AFF = 1 << 2;
    public static final int IORING_SETUP_CQSIZE = 1 << 3;
    public static final int IORING_SETUP_CLAMP = 1 << 4;
    public static final int IORING_SETUP_ATTACH_WQ = 1 << 5;
    public static final int IORING_SETUP_R_DISABLED = 1 << 6;
    public static final int IORING_SETUP_SUBMIT_ALL = 1 << 7;
    public static final int IORING_SETUP_COOP_TASKRUN = 1 << 8;
    public static final int IORING_SETUP_TASKRUN_FLAG = 1 << 9;
    public static final int IORING_SETUP_SQE128 = 1 << 10;
    public static final int IORING_SETUP_CQE32 = 1 << 11;
    public static final int IORING_SETUP_SINGLE_ISSUER = 1 << 12;
    public static final int IORING_SETUP_DEFER_TASKRUN = 1 << 13;

    public static final int IORING_ENTER_GETEVENTS = 1 << 0;
    public static final int IORING_ENTER_SQ_WAKEUP = 1 << 1;
    public static final int IORING_ENTER_SQ_WAIT = 1 << 2;
    public static final int IORING_ENTER_EXT_ARG = 1 << 3;
    public static final int IORING_ENTER_REGISTERED_RING = 1 << 4;

    public static final int IORING_FEAT_SINGLE_MMAP = 1 << 0;
    public static final int IORING_FEAT_NODROP = 1 << 1;
    public static final int IORING_FEAT_SUBMIT_STABLE = 1 << 2;
    public static final int IORING_FEAT_RW_CUR_POS = 1 << 3;
    public static final int IORING_FEAT_CUR_PERSONALITY = 1 << 4;
    public static final int IORING_FEAT_FAST_POLL = 1 << 5;
    public static final int IORING_FEAT_POLL_32BITS = 1 << 6;
    public static final int IORING_FEAT_SQPOLL_NONFIXED = 1 << 7;
    public static final int IORING_FEAT_EXT_ARG = 1 << 8;
    public static final int IORING_FEAT_NATIVE_WORKERS = 1 << 9;
    public static final int IORING_FEAT_RSRC_TAGS = 1 << 10;
    public static final int IORING_FEAT_CQE_SKIP = 1 << 11;
    public static final int IORING_FEAT_LINKED_FILE = 1 << 12;

    public static final int IOSQE_FIXED_FILE = 1 << 0;
    public static final int IOSQE_IO_DRAIN = 1 << 1;
    public static final int IOSQE_IO_LINK = 1 << 2;
    public static final int IOSQE_IO_HARDLINK = 1 << 3;
    public static final int IOSQE_ASYNC = 1 << 4;
    public static final int IOSQE_BUFFER_SELECT = 1 << 5;
    public static final int IOSQE_CQE_SKIP_SUCCESS = 1 << 6;

    public final static int IORING_CQE_F_BUFFER = 1 << 0;
    public final static int IORING_CQE_F_MORE = 1 << 1;
    public final static int IORING_CQE_F_SOCK_NONEMPTY = 1 << 2;
    public final static int IORING_CQE_F_NOTIF = 1 << 3;

    public final static int IORING_FSYNC_DATASYNC = 1 << 0;

    public final static int IORING_REGISTER_BUFFERS = 0;
    public final static int IORING_UNREGISTER_BUFFERS = 1;
    public final static int IORING_REGISTER_FILES = 2;
    public final static int IORING_UNREGISTER_FILES = 3;
    public final static int IORING_REGISTER_EVENTFD = 4;
    public final static int IORING_UNREGISTER_EVENTFD = 5;
    public final static int IORING_REGISTER_FILES_UPDATE = 6;
    public final static int IORING_REGISTER_EVENTFD_ASYNC = 7;
    public final static int IORING_REGISTER_PROBE = 8;
    public final static int IORING_REGISTER_PERSONALITY = 9;
    public final static int IORING_UNREGISTER_PERSONALITY = 10;
    public final static int IORING_REGISTER_RESTRICTIONS = 11;
    public final static int IORING_REGISTER_ENABLE_RINGS = 12;

    /* extended with tagging */
    public final static int IORING_REGISTER_FILES2 = 13;
    public final static int IORING_REGISTER_FILES_UPDATE2 = 14;
    public final static int IORING_REGISTER_BUFFERS2 = 15;
    public final static int IORING_REGISTER_BUFFERS_UPDATE = 16;

    /* set/clear io-wq thread affinities */
    public final static int IORING_REGISTER_IOWQ_AFF = 17;
    public final static int IORING_UNREGISTER_IOWQ_AFF = 18;

    /* set/get max number of io-wq workers */
    public final static int IORING_REGISTER_IOWQ_MAX_WORKERS = 19;

    /* register/unregister io_uring fd with the ring */
    public final static int IORING_REGISTER_RING_FDS = 20;
    public final static int IORING_UNREGISTER_RING_FDS = 21;

    /* register ring based provide buffer group */
    public final static int IORING_REGISTER_PBUF_RING = 22;
    public final static int IORING_UNREGISTER_PBUF_RING = 23;

    /* sync cancelation API */
    public final static int IORING_REGISTER_SYNC_CANCEL = 24;

    /* register a range of fixed file slots for automatic slot allocation */
    public final static int IORING_REGISTER_FILE_ALLOC_RANGE = 25;


    public final static int IORING_RECVSEND_POLL_FIRST = 1 << 0;
    public final static int IORING_RECV_MULTISHOT = 1 << 1;
    public final static int IORING_RECVSEND_FIXED_BUF = 1 << 2;
    long ringAddr;
    int ringFd;
    int enterRingFd;
    int features;

    private boolean closed = false;
    private final SubmissionQueue sq;
    private final CompletionQueue cq;

    // private final SubmissionQueue sq = new SubmissionQueue();
    // https://man.archlinux.org/man/io_uring.7.en

    /**
     * Creates a new IoUring with the given number of entries.
     *
     * @param entries the number of entries in the ring.
     * @throws IllegalArgumentException when entries smaller than 1
     */
    public IOUring(int entries, int flags) {
        checkPositive(entries, "entries must be larger than 0");
        checkNotNegative(flags, "flags can't be smaller than 0");
        if ((flags & IORING_SETUP_SQE128) != 0) {
            throw new IllegalArgumentException("IORING_SETUP_SQE128 can't be set");
        }
        if ((flags & IORING_SETUP_CQE32) != 0) {
            throw new IllegalArgumentException("IORING_SETUP_CQE32 can't be set");
        }

        init(entries, flags);
        sq = new SubmissionQueue(this);
        cq = new CompletionQueue(this);
        sq.init(ringAddr);
        cq.init(ringAddr);
    }

    private native void init(int entries, int flags);

    public int features() {
        return features;
    }

    public int ringFd() {
        return ringFd;
    }

    public SubmissionQueue getSubmissionQueue() {
        return sq;
    }

    public CompletionQueue getCompletionQueue() {
        return cq;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        closed = true;
        exit(ringAddr);
        ringAddr = 0;
        ringFd = -1;
        sq.onClose();
        cq.onClose();
    }

    /**
     * Registers the file descriptor of the ring. THis makes enter cheaper. There is a limit
     * on the number of IOURing instances that can be registered (16).
     * <p>
     * For more info see:
     * https://man7.org/linux/man-pages/man3/io_uring_register_ring_fd.3.html
     */
    public void registerRingFd() {
        enterRingFd = registerRingFd(ringAddr);
        sq.enterRingFd = enterRingFd;
        sq.ringBufferRegistered = true;
    }

    /**
     * Unregisters the file descriptor of the ring.
     * <p/>
     * For more info see:
     * // https://man7.org/linux/man-pages/man3/io_uring_unregister_ring_fd.3.html
     */
    public void unregisterRingFd() {
        unregisterRingFd(ringAddr);
    }

    public void register(int opcode, long arg, int nr_args) {
        register(ringFd, opcode, arg, nr_args);
    }
//
//    public void registerSocket(int socket_fd){
//        register_socket(ringFd, socket_fd);
//    }

    //public static native void register_socket(int ring_fd, int socket_fd);

    public static native void register(int fd, int opcode, long arg, int nr_args);

    public static native int registerRingFd(long ring_addr);

    public static native void unregisterRingFd(long ring_addr);

    public static native int enter(int ringFd, int toSubmit, int minComplete, int flags);

    private static native void exit(long ring);
}