/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_hazelcast_internal_tpcengine_iouring_IOUring */

#ifndef _Included_com_hazelcast_internal_tpcengine_iouring_IOUring
#define _Included_com_hazelcast_internal_tpcengine_iouring_IOUring
#ifdef __cplusplus
extern "C" {
#endif
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_NOP
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_NOP 0L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_READV
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_READV 1L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_WRITEV
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_WRITEV 2L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_FSYNC
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_FSYNC 3L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_READ_FIXED
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_READ_FIXED 4L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_WRITE_FIXED
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_WRITE_FIXED 5L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_POLL_ADD
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_POLL_ADD 6L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_POLL_REMOVE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_POLL_REMOVE 7L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SYNC_FILE_RANGE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SYNC_FILE_RANGE 8L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SENDMSG
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SENDMSG 9L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_RECVMSG
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_RECVMSG 10L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_TIMEOUT
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_TIMEOUT 11L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_TIMEOUT_REMOVE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_TIMEOUT_REMOVE 12L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_ACCEPT
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_ACCEPT 13L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_ASYNC_CANCEL
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_ASYNC_CANCEL 14L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_LINK_TIMEOUT
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_LINK_TIMEOUT 15L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_CONNECT
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_CONNECT 16L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_FALLOCATE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_FALLOCATE 17L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_OPENAT
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_OPENAT 18L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_CLOSE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_CLOSE 19L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_FILES_UPDATE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_FILES_UPDATE 20L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_STATX
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_STATX 21L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_READ
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_READ 22L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_WRITE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_WRITE 23L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_FADVISE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_FADVISE 24L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_MADVISE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_MADVISE 25L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SEND
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SEND 26L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_RECV
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_RECV 27L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_OPENAT2
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_OPENAT2 28L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_EPOLL_CTL
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_EPOLL_CTL 29L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SPLICE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SPLICE 30L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_PROVIDE_BUFFERS
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_PROVIDE_BUFFERS 31L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_REMOVE_BUFFERS
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_REMOVE_BUFFERS 32L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_TEE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_TEE 33L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SHUTDOWN
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SHUTDOWN 34L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_RENAMEAT
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_RENAMEAT 35L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_UNLINKAT
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_UNLINKAT 36L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_MKDIRAT
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_MKDIRAT 37L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SYMLINKAT
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SYMLINKAT 38L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_LINKAT
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_LINKAT 39L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_MSG_RING
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_MSG_RING 40L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_FSETXATTR
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_FSETXATTR 41L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SETXATTR
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SETXATTR 42L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_FGETXATTR
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_FGETXATTR 43L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_GETXATTR
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_GETXATTR 44L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SOCKET
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SOCKET 45L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_URING_CMD
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_URING_CMD 46L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SEND_ZC
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SEND_ZC 47L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SENDMSG_ZC
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_OP_SENDMSG_ZC 48L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_IOPOLL
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_IOPOLL 1L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_SQPOLL
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_SQPOLL 2L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_SQ_AFF
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_SQ_AFF 4L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_CQSIZE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_CQSIZE 8L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_CLAMP
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_CLAMP 16L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_ATTACH_WQ
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_ATTACH_WQ 32L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_R_DISABLED
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_R_DISABLED 64L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_SUBMIT_ALL
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_SUBMIT_ALL 128L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_COOP_TASKRUN
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_COOP_TASKRUN 256L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_TASKRUN_FLAG
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_TASKRUN_FLAG 512L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_SQE128
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_SQE128 1024L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_CQE32
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_CQE32 2048L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_SINGLE_ISSUER
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_SINGLE_ISSUER 4096L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_DEFER_TASKRUN
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_SETUP_DEFER_TASKRUN 8192L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_ENTER_GETEVENTS
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_ENTER_GETEVENTS 1L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_ENTER_SQ_WAKEUP
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_ENTER_SQ_WAKEUP 2L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_ENTER_SQ_WAIT
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_ENTER_SQ_WAIT 4L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_ENTER_EXT_ARG
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_ENTER_EXT_ARG 8L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_ENTER_REGISTERED_RING
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_ENTER_REGISTERED_RING 16L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_SINGLE_MMAP
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_SINGLE_MMAP 1L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_NODROP
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_NODROP 2L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_SUBMIT_STABLE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_SUBMIT_STABLE 4L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_RW_CUR_POS
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_RW_CUR_POS 8L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_CUR_PERSONALITY
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_CUR_PERSONALITY 16L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_FAST_POLL
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_FAST_POLL 32L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_POLL_32BITS
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_POLL_32BITS 64L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_SQPOLL_NONFIXED
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_SQPOLL_NONFIXED 128L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_EXT_ARG
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_EXT_ARG 256L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_NATIVE_WORKERS
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_NATIVE_WORKERS 512L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_RSRC_TAGS
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_RSRC_TAGS 1024L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_CQE_SKIP
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_CQE_SKIP 2048L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_LINKED_FILE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FEAT_LINKED_FILE 4096L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IOSQE_FIXED_FILE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IOSQE_FIXED_FILE 1L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IOSQE_IO_DRAIN
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IOSQE_IO_DRAIN 2L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IOSQE_IO_LINK
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IOSQE_IO_LINK 4L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IOSQE_IO_HARDLINK
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IOSQE_IO_HARDLINK 8L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IOSQE_ASYNC
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IOSQE_ASYNC 16L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IOSQE_BUFFER_SELECT
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IOSQE_BUFFER_SELECT 32L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IOSQE_CQE_SKIP_SUCCESS
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IOSQE_CQE_SKIP_SUCCESS 64L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_CQE_F_BUFFER
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_CQE_F_BUFFER 1L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_CQE_F_MORE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_CQE_F_MORE 2L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_CQE_F_SOCK_NONEMPTY
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_CQE_F_SOCK_NONEMPTY 4L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_CQE_F_NOTIF
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_CQE_F_NOTIF 8L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FSYNC_DATASYNC
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_FSYNC_DATASYNC 1L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_BUFFERS
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_BUFFERS 0L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_UNREGISTER_BUFFERS
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_UNREGISTER_BUFFERS 1L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_FILES
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_FILES 2L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_UNREGISTER_FILES
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_UNREGISTER_FILES 3L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_EVENTFD
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_EVENTFD 4L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_UNREGISTER_EVENTFD
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_UNREGISTER_EVENTFD 5L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_FILES_UPDATE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_FILES_UPDATE 6L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_EVENTFD_ASYNC
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_EVENTFD_ASYNC 7L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_PROBE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_PROBE 8L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_PERSONALITY
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_PERSONALITY 9L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_UNREGISTER_PERSONALITY
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_UNREGISTER_PERSONALITY 10L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_RESTRICTIONS
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_RESTRICTIONS 11L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_ENABLE_RINGS
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_ENABLE_RINGS 12L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_FILES2
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_FILES2 13L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_FILES_UPDATE2
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_FILES_UPDATE2 14L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_BUFFERS2
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_BUFFERS2 15L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_BUFFERS_UPDATE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_BUFFERS_UPDATE 16L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_IOWQ_AFF
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_IOWQ_AFF 17L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_UNREGISTER_IOWQ_AFF
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_UNREGISTER_IOWQ_AFF 18L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_IOWQ_MAX_WORKERS
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_IOWQ_MAX_WORKERS 19L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_RING_FDS
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_RING_FDS 20L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_UNREGISTER_RING_FDS
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_UNREGISTER_RING_FDS 21L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_PBUF_RING
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_PBUF_RING 22L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_UNREGISTER_PBUF_RING
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_UNREGISTER_PBUF_RING 23L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_SYNC_CANCEL
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_SYNC_CANCEL 24L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_FILE_ALLOC_RANGE
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_REGISTER_FILE_ALLOC_RANGE 25L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_RECVSEND_POLL_FIRST
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_RECVSEND_POLL_FIRST 1L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_RECV_MULTISHOT
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_RECV_MULTISHOT 2L
#undef com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_RECVSEND_FIXED_BUF
#define com_hazelcast_internal_tpcengine_iouring_IOUring_IORING_RECVSEND_FIXED_BUF 4L
/*
 * Class:     com_hazelcast_internal_tpcengine_iouring_IOUring
 * Method:    init
 * Signature: (II)V
 */
JNIEXPORT void JNICALL Java_com_hazelcast_internal_tpcengine_iouring_IOUring_init
  (JNIEnv *, jobject, jint, jint);

/*
 * Class:     com_hazelcast_internal_tpcengine_iouring_IOUring
 * Method:    register
 * Signature: (IIJI)V
 */
JNIEXPORT void JNICALL Java_com_hazelcast_internal_tpcengine_iouring_IOUring_register
  (JNIEnv *, jclass, jint, jint, jlong, jint);

/*
 * Class:     com_hazelcast_internal_tpcengine_iouring_IOUring
 * Method:    registerRingFd
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_com_hazelcast_internal_tpcengine_iouring_IOUring_registerRingFd
  (JNIEnv *, jclass, jlong);

/*
 * Class:     com_hazelcast_internal_tpcengine_iouring_IOUring
 * Method:    unregisterRingFd
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_hazelcast_internal_tpcengine_iouring_IOUring_unregisterRingFd
  (JNIEnv *, jclass, jlong);

/*
 * Class:     com_hazelcast_internal_tpcengine_iouring_IOUring
 * Method:    enter
 * Signature: (IIII)I
 */
JNIEXPORT jint JNICALL Java_com_hazelcast_internal_tpcengine_iouring_IOUring_enter
  (JNIEnv *, jclass, jint, jint, jint, jint);

/*
 * Class:     com_hazelcast_internal_tpcengine_iouring_IOUring
 * Method:    exit
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_hazelcast_internal_tpcengine_iouring_IOUring_exit
  (JNIEnv *, jclass, jlong);

#ifdef __cplusplus
}
#endif
#endif
