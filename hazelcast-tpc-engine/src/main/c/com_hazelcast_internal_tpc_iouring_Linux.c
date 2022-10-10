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

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <time.h>
#include <errno.h>
#include "include/utils.h"
#include "include/com_hazelcast_internal_tpc_iouring_Linux.h"

JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_errno(JNIEnv *env,jclass this_class){
    return errno;
}

JNIEXPORT jstring JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_strerror(JNIEnv* env, jclass this_class, jint errnum){
    char* err_msg = strerror(errnum);
    return (*env)->NewStringUTF(env, err_msg);
}

JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_eventfd(JNIEnv* env, jclass this_class, jint initval,
                                                      jint flags){
    return eventfd(initval, flags);
}

JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_open(JNIEnv* env, jclass this_class, jstring pathname,
                                                   jlong flags, jint mode){
    char* path = (*env)->GetStringUTFChars( env, pathname, NULL );
    return open(path, (int)flags, (mode_t)mode);
}


JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_close(JNIEnv* env, jclass this_class, jint fd){
    return close(fd);
}

JNIEXPORT jlong JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_read(JNIEnv* env, jclass this_class, jint fd, jlong buf,
                                                   jlong count){
    return read(fd, (void *)buf, (size_t)count);
}

JNIEXPORT jlong JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_write(JNIEnv* env, jclass this_class, jint fd, jlong buf,
                                                    jlong count){
    return write(fd, (void *)buf, (size_t)count);
}

JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_eventfd_1write(JNIEnv* env, jclass this_class, jint fd,
                                                             jlong value){
    return eventfd_write(fd, value);
}

JNIEXPORT jlong JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_pread(JNIEnv* env, jclass this_class, jint fd, jlong buf,
                                                    jlong count, jlong offset){
    return pread(fd, (void *)buf, (size_t)count, (off_t) offset);
}

JNIEXPORT jlong JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_pwrite(JNIEnv* env, jclass this_class, jint fd, jlong buf,
                                                     jlong count, jlong offset){
    return pwrite(fd, (void *)buf, (size_t)count, (off_t) offset);
}

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_sync(JNIEnv* env, jclass this_class){
    sync();
}

JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_syncfs(JNIEnv* env, jclass this_class, jint fd){
    return syncfs(fd);
}

JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_fsync(JNIEnv* env, jclass this_class, jint fd){
    return fsync(fd);
}

JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_fdatasync(JNIEnv* env, jclass this_class, jint fd){
    return fdatasync(fd);
}
JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_clock_1gettime(JNIEnv* env, jclass this_class, jint clk_id,
                                                             jlong tp){
    return 0;// return clock_gettime(clk_id, );
}

JNIEXPORT jlong JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_clock_1currentTime(JNIEnv* env, jclass this_class){
    struct timespec timespec;

    int res = clock_gettime(CLOCK_REALTIME, &timespec);
    if (res < 0) {
        return throw_exception(env, "clock_gettime", -res);
    }
    return (uint64_t) timespec.tv_sec * 1000000000 + timespec.tv_nsec;
}

JNIEXPORT jlong JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_clock_1monotonicTime(JNIEnv* env,jclass this_class){
    struct timespec timespec;

    int res = clock_gettime(CLOCK_MONOTONIC, &timespec);
    if (res < 0) {
        return throw_exception(env, "clock_gettime:CLOCK_MONOTONIC", -res);
    }
    return (uint64_t) timespec.tv_sec * 1000000000 + timespec.tv_nsec;
}

JNIEXPORT jlong JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_clock_1processCpuTime(JNIEnv* env, jclass this_class){
    struct timespec timespec;

    int res = clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &timespec);
    if (res < 0) {
        return throw_exception(env, "clock_gettime:CLOCK_PROCESS_CPUTIME_ID", -res);
    }
    return (uint64_t) timespec.tv_sec * 1000000000 + timespec.tv_nsec;
}

JNIEXPORT jlong JNICALL
Java_com_hazelcast_internal_tpc_iouring_Linux_clock_1threadCpuTime(JNIEnv* env, jclass this_class){
    struct timespec timespec;

    int res = clock_gettime(CLOCK_MONOTONIC, &timespec);
    if (res < 0 ) {
        return throw_exception(env, "clock_gettime:CLOCK_THREAD_CPUTIME_ID", -res);
    }
    return (uint64_t) timespec.tv_sec * 1000000000 + timespec.tv_nsec;
}