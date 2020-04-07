/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import java.security.SecureRandom;
import java.util.Random;

/**
 * A random number generator isolated to the current thread.
 */
public final class ThreadLocalRandomProvider {

    private static final Random SEED_GENERATOR = new Random();

    private static final ThreadLocal<Random> THREAD_LOCAL_RANDOM = new ThreadLocal<Random>();
    private static final ThreadLocal<SecureRandom> THREAD_LOCAL_SECURE_RANDOM = new ThreadLocal<SecureRandom>();

    private ThreadLocalRandomProvider() {
    }

    /**
     * Returns the current thread's {@link Random}.
     *
     * @return the current thread's {@link Random}.
     */
    public static Random get() {
        Random random = THREAD_LOCAL_RANDOM.get();
        if (random == null) {
            long seed = SEED_GENERATOR.nextLong();
            random = new Random(seed);
            THREAD_LOCAL_RANDOM.set(random);
        }
        return random;
    }

    /**
     * Returns the current thread's {@link SecureRandom}.
     *
     * @return the current thread's {@link SecureRandom}.
     */
    public static SecureRandom getSecure() {
        SecureRandom random = THREAD_LOCAL_SECURE_RANDOM.get();
        if (random == null) {
            random = new SecureRandom();
            THREAD_LOCAL_SECURE_RANDOM.set(random);
        }
        return random;
    }
}
