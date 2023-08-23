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

package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.tpcengine.util.ExceptionUtil;
import org.junit.Test;

/**
 * A test that verifies that a reactor can be created and destroyed many
 * times after each other. The NioReactor should be fine since the resources
 * will be gc's by the JVM automatically, but it will help to flush out problems
 * in the UringReactor because of unsafe memory allocation, OS resources that need
 * to be released like the uring, eventfd etc.
 * <p>
 * There are short running tests for the regular tests, and there are also nightly
 * version that run much longer.
 */
public abstract class Reactor_CreateDestroyTest {

    protected long iterations = 1000;

    public abstract Reactor.Builder newReactorBuilder();

    public Reactor newReactor() {
        Reactor.Builder builder = newReactorBuilder();
        // Make sure that we create a uring instance that doesn't
        // have a lot of capacity. This will flush out problems faster.
        // And this also creates just a little bit of space for listeners
        // in the completion queue. Exposing problems faster.
        builder.socketsLimit = 1;
        builder.fileLimit = 1;
        builder.storagePendingLimit = 1;
        builder.storageSubmitLimit = 1;
        builder.serverSocketsLimit = 1;
        Reactor reactor = builder.build();
        return reactor;
    }

    @Test
    public void test_whenEmptyReactor() {
        for (long iteration = 0; iteration < iterations; iteration++) {
            if (iteration % 1000 == 0) {
                System.out.println("at iteration:" + iteration);
            }

            try {
                Reactor reactor = newReactor();
                reactor.start();
                reactor.shutdown();
                TpcTestSupport.terminate(reactor);
            } catch (Throwable t) {
                System.out.println("Problem detected at iteration " + iteration + ".");
                throw ExceptionUtil.sneakyThrow(t);
            }
        }
    }
}
