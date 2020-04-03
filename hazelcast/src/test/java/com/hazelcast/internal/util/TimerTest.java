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

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TimerTest {

    public static final long INCREMENT = 1_000_000_000L;

    @Test
    public void testElapsedSince() {
        Timer timer = Timer.create(new NanoClockMock(0));
        long time = timer.nanos();

        assertThat(timer.nanosElapsedSince(time), is(equalTo(INCREMENT)));
        assertThat(timer.millisElapsedSince(time), is(equalTo(2 * TimeUnit.NANOSECONDS.toMillis(INCREMENT))));
    }

    @Test
    public void testNanoOverflow() {
        Timer timer = Timer.create(new NanoClockMock(Long.MAX_VALUE - INCREMENT / 2));
        long time = timer.nanos();

        assertThat(timer.nanosElapsedSince(time), is(equalTo(INCREMENT)));
        assertThat(timer.millisElapsedSince(time), is(equalTo(2 * TimeUnit.NANOSECONDS.toMillis(INCREMENT))));
    }

    private static class NanoClockMock implements Timer.NanoClock {
        private long nano;

        NanoClockMock(long initialNano) {
            nano = initialNano;
        }

        @Override
        public long nanoTime() {
            long now = nano;
            nano += INCREMENT;
            return now;
        }
    }
}
