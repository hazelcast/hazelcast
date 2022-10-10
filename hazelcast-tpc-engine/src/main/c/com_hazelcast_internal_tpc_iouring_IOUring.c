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

#include <liburing.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "include/com_hazelcast_internal_tpc_iouring_IOUring.h"
#include "include/io_uring.h"
#include "include/utils.h"

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_IOUring_init(JNIEnv* env, jobject this_object,
                                                     jint entries, jint flags){
    struct io_uring *p_ring = malloc(sizeof(struct io_uring));

    if (!p_ring) {
        throw_out_of_memory_error(env);
        return;
    }

    struct io_uring_params params;
    memset(&params, 0, sizeof(struct io_uring_params));
    params.flags = flags;

    int ret = io_uring_queue_init_params(entries, p_ring, &params);
    if (ret < 0) {
        throw_exception(env, "io_uring_queue_init", -ret);
        return;
    }

    jclass this_class = (*env)->GetObjectClass(env, this_object);

    jfieldID ringAddr_FieldId = (*env)->GetFieldID(env, this_class, "ringAddr","J");
    (*env)->SetLongField(env, this_object, ringAddr_FieldId, (jlong) p_ring);

    jfieldID ringFd_FieldId = (*env)->GetFieldID(env, this_class, "ringFd","I");
    (*env)->SetIntField(env, this_object, ringFd_FieldId, (jint) p_ring->ring_fd);

    jfieldID enter_ringFd_FieldId = (*env)->GetFieldID(env, this_class, "enterRingFd","I");
    (*env)->SetIntField(env, this_object, enter_ringFd_FieldId, (jint) p_ring->ring_fd);

    jfieldID features_FieldId = (*env)->GetFieldID(env, this_class, "features","I");
    (*env)->SetIntField(env, this_object, features_FieldId, (jint) params.features);
}

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_IOUring_register(JNIEnv* env, jclass this_class, jint fd,
                                                         jint opcode, jlong arg, jint nr_args){
     int res = io_uring_register(fd, opcode, (void *)arg, nr_args);
     if (res < 0){
        return throw_exception(env, "io_uring_enter", -res);
     }
}

JNIEXPORT int JNICALL
Java_com_hazelcast_internal_tpc_iouring_IOUring_registerRingFd(JNIEnv* env, jclass this_class,
                                                               jlong ring_addr){
    // https://man7.org/linux/man-pages/man3/io_uring_register_ring_fd.3.html
    int res = io_uring_register_ring_fd((struct io_uring *)ring_addr);
    if (res != 1){
        return throw_exception(env, "io_uring_register_ring_fd", -res);
    }

    return ((struct io_uring *)ring_addr)->enter_ring_fd;
}

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_IOUring_unregisterRingFd(JNIEnv* env, jclass this_class,
                                                               jlong ring_addr){
    // https://man7.org/linux/man-pages/man3/io_uring_unregister_ring_fd.3.html
    int res = io_uring_unregister_ring_fd((struct io_uring *)ring_addr);
    if (res != 1){
        return throw_exception(env, "io_uring_unregister_ring_fd", -res);
    }
}


//JNIEXPORT void JNICALL
//Java_com_hazelcast_internal_tpc_iouring_IOUring_register_1socket(JNIEnv* env, jclass this_class,
//                                                                jint ring_fd, jint socket_fd){
//
//     int res = io_uring_register(fd, IORING_REGISTER_FILES, (void *)arg, 1);
//     if (res < 0){
//        return throw_exception(env, "io_uring_enter", errno);
//     }
//}

JNIEXPORT jint JNICALL
Java_com_hazelcast_internal_tpc_iouring_IOUring_enter(JNIEnv* env, jclass this_class, jint fd,
                                                      jint to_submit, jint min_complete, jint flags){
    int res = io_uring_enter(fd, to_submit, min_complete, flags, NULL);
    if (res < 0){
        if(res == -EINTR){
            // dealing with interrupt.
            return 0;
        }else {
            // don't rely on errno:
            // https://manpages.debian.org/unstable/liburing-dev/io_uring_enter.2.en.html
            return throw_exception(env, "io_uring_enter", -res);
        }
    }
    return res;
}

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_IOUring_exit(JNIEnv* env, jclass this_class, jlong ring){
    io_uring_queue_exit((struct io_uring *)ring);
}
