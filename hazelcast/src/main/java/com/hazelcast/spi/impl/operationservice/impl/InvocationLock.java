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

package com.hazelcast.spi.impl.operationservice.impl;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class InvocationLock {
    private static final AtomicInteger TOTAL_DO_INVOKE_COUNT = new AtomicInteger();
    private static final Set<RequestOwner> LOCK_REQUESTS = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Semaphore doInvokeMutex = new Semaphore(1);

    boolean canEnterDoInvoke() {
        if (LOCK_REQUESTS.size() > 0) {
            return false;
        }

        if (doInvokeMutex.tryAcquire()) {
            TOTAL_DO_INVOKE_COUNT.getAndIncrement();
            if (LOCK_REQUESTS.size() > 0) {
                exitDoInvoke();
                return false;
            }
            return true;
        }

        return false;
    }

    void exitDoInvoke() {
        doInvokeMutex.release();
        TOTAL_DO_INVOKE_COUNT.decrementAndGet();
    }

    static boolean canLoopInvocationRegistry(RequestOwner requestOwner) {
        LOCK_REQUESTS.add(requestOwner);
        return TOTAL_DO_INVOKE_COUNT.get() == 0;
    }

    static void afterInvocationRegistryLoop(RequestOwner requestOwner) {
        LOCK_REQUESTS.remove(requestOwner);
    }

    enum RequestOwner {
        ON_MEMBER_LEFT,
        ON_ENDPOINT_LEFT
    }
}
