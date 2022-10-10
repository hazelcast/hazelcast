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

#include "include/com_hazelcast_internal_tpc_iouring_CompletionQueue.h"
#include "include/io_uring.h"
#include "include/utils.h"

JNIEXPORT void JNICALL
Java_com_hazelcast_internal_tpc_iouring_CompletionQueue_init(JNIEnv* env, jobject this_object, jlong io_uring){

    jclass this_class = (*env)->GetObjectClass(env, this_object);

    //https://github.com/axboe/liburing/blob/master/src/include/liburing.h

    struct io_uring_cq *cq = &(((struct io_uring*) io_uring)->cq);

    jfieldID headAddr_FieldId = (*env)->GetFieldID(env, this_class, "headAddr","J");
    (*env)->SetLongField(env, this_object, headAddr_FieldId, (jlong) cq->khead);

    jfieldID tailAddr_FieldId = (*env)->GetFieldID(env, this_class, "tailAddr","J");
    (*env)->SetLongField(env, this_object, tailAddr_FieldId, (jlong) cq->ktail);

    jfieldID cqesAddr_FieldId = (*env)->GetFieldID(env, this_class, "cqesAddr","J");
    (*env)->SetLongField(env, this_object, cqesAddr_FieldId, (jlong) cq->cqes);

    jfieldID ringMask_FieldId = (*env)->GetFieldID(env, this_class, "ringMask","I");
    (*env)->SetIntField(env, this_object, ringMask_FieldId, (jint) cq->ring_mask);
}