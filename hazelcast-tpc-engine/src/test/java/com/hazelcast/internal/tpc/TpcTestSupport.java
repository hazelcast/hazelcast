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

package com.hazelcast.internal.tpc;

import com.hazelcast.test.AssertTask;
import com.hazelcast.test.TestSupport;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

public class TpcTestSupport extends TestSupport {

    public static final int TERMINATION_TIMEOUT_SECONDS = 30;

    public static void assertCompletesEventually(final Future future) {
        assertTrueEventually(() -> assertTrue("Future has not completed", future.isDone()));
    }

    public static void assertOpenEventually(CountDownLatch latch) {
        assertOpenEventually(latch, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertOpenEventually(CountDownLatch latch, long timeoutSeconds) {
        assertTrueEventually(() -> {
            boolean success = latch.await(timeoutSeconds, SECONDS);
            assertTrue(success);
        }, timeoutSeconds);
    }

    public static void assertTrueFiveSeconds(AssertTask task) {
        assertTrueAllTheTime(task, 5);
    }

    public static void assertTrueTwoSeconds(AssertTask task) {
        assertTrueAllTheTime(task, 2);
    }

    public static void terminateAll(Collection<? extends Reactor> reactors) {
        if (reactors == null) {
            return;
        }

        for (Reactor reactor : reactors) {
            if (reactor == null) {
                continue;
            }
            reactor.shutdown();
        }

        for (Reactor reactor : reactors) {
            if (reactor == null) {
                continue;
            }
            try {
                if (!reactor.awaitTermination(TERMINATION_TIMEOUT_SECONDS, SECONDS)) {
                    throw new RuntimeException("Reactor failed to terminate within timeout.");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void terminate(Reactor reactor) {
        if (reactor == null) {
            return;
        }

        reactor.shutdown();
        try {
            if (!reactor.awaitTermination(TERMINATION_TIMEOUT_SECONDS, SECONDS)) {
                throw new RuntimeException("Reactor failed to terminate within timeout.");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
