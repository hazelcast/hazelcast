/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

public final class ThreadUtil {

    private static ThreadLocal<Integer> threadLocal = new ThreadLocal<Integer>();

    private ThreadUtil(){}

    public static int getThreadId() {
        final Integer threadId = threadLocal.get();
        if (threadId != null) {
            return threadId;
        }
        return (int) Thread.currentThread().getId();  // TODO: @mm - thread-id is truncated from native thread id
    }

    public static void setThreadId(long threadId) {
        threadLocal.set((int)threadId);
    }

    public static void removeThreadId() {
        threadLocal.remove();
    }

}
