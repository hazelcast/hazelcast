/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

class FrequentLogSuppressorTest {

    private static final ILogger logger = Logger.getLogger(FrequentLogSuppressorTest.class);

    @Test
    void shouldRunAction() {
        FrequentLogSuppressor suppressor = new FrequentLogSuppressor(10, 3, logger, new HashMap<>());
        RuntimeException e = new RuntimeException("exception");

        AtomicBoolean run = new AtomicBoolean();
        suppressor.runSuppressed(e, () -> run.set(true));
        assertThat(run.get())
                .describedAs("expected the action to run")
                .isTrue();
    }

    @Test
    void shouldRunActionForDifferentExceptions() {
        FrequentLogSuppressor suppressor = new FrequentLogSuppressor(10, 3, logger, new HashMap<>());
        RuntimeException e1 = new RuntimeException("exception 1");
        RuntimeException e2 = new RuntimeException("exception 2");

        AtomicInteger counter = new AtomicInteger();
        suppressor.runSuppressed(e1, counter::incrementAndGet);
        suppressor.runSuppressed(e2, counter::incrementAndGet);
        assertThat(counter.get())
                .describedAs("expected both action to run")
                .isEqualTo(2);
    }

    @Test
    void shouldPrintSuppressedMessage() {
        ILogger spy = Mockito.spy(logger);
        FrequentLogSuppressor suppressor = new FrequentLogSuppressor(10, 3, spy, new HashMap<>());
        RuntimeException e = new RuntimeException("exception");

        for (int i = 0; i < 3; i++) {
            suppressor.runSuppressed(e, () -> { /* no-op */});
        }
        verify(spy).warning(eq("Frequent log operation detected, "
                               + "future occurrences will be suppressed and only logged every 10 seconds"));
    }

    @Test
    void shouldSuppressAdditionalActions() {
        FrequentLogSuppressor suppressor = new FrequentLogSuppressor(10, 3, logger, new HashMap<>());
        RuntimeException e = new RuntimeException("exception");

        AtomicInteger counter = new AtomicInteger();
        for (int i = 0; i < 5; i++) {
            suppressor.runSuppressed(e, counter::incrementAndGet);
        }
        assertThat(counter.get())
                .describedAs("expected the action to run only 3 times")
                .isEqualTo(3);
    }

    @Test
    void shouldLogAndRunActionAfterLogPeriod() throws Exception {
        ILogger spy = Mockito.spy(logger);
        FrequentLogSuppressor suppressor = new FrequentLogSuppressor(1, 3, spy, new HashMap<>());
        RuntimeException e = new RuntimeException("exception");

        AtomicInteger counter = new AtomicInteger();
        for (int i = 0; i < 5; i++) {
            // Runs 3 times here
            suppressor.runSuppressed(e, counter::incrementAndGet);
        }
        Thread.sleep(1_200);

        // Runs once more here
        suppressor.runSuppressed(e, counter::incrementAndGet);

        assertThat(counter.get())
                .describedAs("expected the action to run 4 times")
                .isEqualTo(4);

        verify(spy).warning(eq("The following suppressed log had 3 occurrences since last log, 6 in total"));
    }
}
