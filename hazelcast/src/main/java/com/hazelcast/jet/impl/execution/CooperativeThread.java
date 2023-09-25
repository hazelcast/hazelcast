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

package com.hazelcast.jet.impl.execution;

/**
 * A marker interface used for Jet cooperative {@link Thread}s. Some code known
 * to not being cooperative can check it and fail if it's running on such a
 * thread. It can check using {@link #checkNonCooperative()}.
 */
public interface CooperativeThread {

    /**
     * Throws an error if the current thread is a cooperative thread.
     */
    static void checkNonCooperative() {
        if (Thread.currentThread() instanceof CooperativeThread) {
            throw new RuntimeException("A non-cooperative task attempting to run on a cooperative thread");
        }
    }
}
