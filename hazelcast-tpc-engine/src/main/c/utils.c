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

#include <string.h>
#include "include/utils.h"

int32_t throw_exception(JNIEnv *env, char *cause, int32_t errno) {
    char error_msg[1024];
    snprintf(error_msg, sizeof(error_msg), "%s - %s", cause, strerror(errno));
    return (*env)->ThrowNew(env, (*env)->FindClass(env, "java/lang/RuntimeException"), &error_msg);
}

int32_t throw_io_exception(JNIEnv *env, char *cause, int32_t errno) {
    char error_msg[1024];
    snprintf(error_msg, sizeof(error_msg), "%s - %s", cause, strerror(errno));
    return (*env)->ThrowNew(env, (*env)->FindClass(env, "java/io/IOException"), &error_msg);
}

int32_t throw_out_of_memory_error(JNIEnv *env) {
    return (*env)->ThrowNew(env, (*env)->FindClass(env, "java/lang/OutOfMemoryError"), "Out of heap space");
}
