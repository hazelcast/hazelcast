package com.hazelcast.internal.util;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

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

        public NanoClockMock(long initialNano) {
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