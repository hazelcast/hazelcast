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

package classloading;

import java.util.Random;

/**
 * This is a demo application which can cause a classloader leakage via {@link ThreadLocal}.
 * <p>
 * It is adapted from original Hazelcast code which created a {@link ThreadLocal} leakage.
 */
public final class LeakingApplication {

    private LeakingApplication() {
    }

    public static void init(Boolean doCleanup) {
        ThreadLocalRandom.LOCAL_RANDOM.get();
        if (doCleanup) {
            cleanup();
        }
    }

    public static void cleanup() {
        ThreadLocalRandom.LOCAL_RANDOM.remove();
    }

    private static class ThreadLocalRandom extends Random {

        /**
         * This causes a classloader leakage which may produce errors in web containers or
         * even cause a PermGen Space OOME in Java 6 (since references are not cleaned up).
         * <p>
         * Never override {@link ThreadLocal#initialValue()} in production code!
         */
        private static final ThreadLocal<ThreadLocalRandom> LOCAL_RANDOM =
                new ThreadLocal<ThreadLocalRandom>() {
                    protected ThreadLocalRandom initialValue() {
                        return new ThreadLocalRandom();
                    }
                };
    }
}
