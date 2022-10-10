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
#include <string.h>
#include "include/com_hazelcast_internal_tpc_iouring_SubmissionQueue.h"
#include "include/io_uring.h"
#include "include/utils.h"


JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_SubmissionQueue_init0(JNIEnv* env, jobject this_object, jlong io_uring){

    jclass this_class = (*env)->GetObjectClass(env, this_object);

    //https://github.com/axboe/liburing/blob/master/src/include/liburing.h

    struct io_uring_sq *sq = &(((struct io_uring*) io_uring)->sq);
//    set_sqe_long_field(env, this_class, this_object, "sqe_head","J",(uint64_t) &(sq->sqe_head));

//    printf("sqe_head %d\n", sq->sqe_head);
//    printf("sqe_tail %d\n", sq->sqe_tail);
//    fflush(stdout);

    jfieldID headAddr_FieldId = (*env)->GetFieldID(env, this_class, "headAddr","J");
    (*env)->SetLongField(env, this_object, headAddr_FieldId, (jlong) sq->khead);

    jfieldID tailAddr_FieldId = (*env)->GetFieldID(env, this_class, "tailAddr","J");
    (*env)->SetLongField(env, this_object, tailAddr_FieldId, (jlong) sq->ktail);

    jfieldID arrayAddr_FieldId = (*env)->GetFieldID(env, this_class, "arrayAddr","J");
    (*env)->SetLongField(env, this_object, arrayAddr_FieldId, (jlong) sq->array);

    jfieldID sqesAddr_FieldId = (*env)->GetFieldID(env, this_class, "sqesAddr","J");
    (*env)->SetLongField(env, this_object, sqesAddr_FieldId, (jlong) sq->sqes);

    jfieldID ringMask_FieldId = (*env)->GetFieldID(env, this_class, "ringMask","I");
    (*env)->SetIntField(env, this_object, ringMask_FieldId, (jint) sq->ring_mask);

    jfieldID ringEntries_FieldId = (*env)->GetFieldID(env, this_class, "ringEntries","I");
    (*env)->SetIntField(env, this_object, ringEntries_FieldId, (jint) sq->ring_entries);
}
