package com.hazelcast.test.jitter;

import com.hazelcast.util.StringUtil;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

import static com.hazelcast.util.QuickMath.nextPowerOfTwo;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * JUnit rule for detecting JVM/OS hiccups. It's meant to give you some environmental insight
 * in the case test failure.
 */
public class JitterRule implements TestRule {

    /**
     * Time interval aggregated into a single bucket. Smaller interval allow
     * a better picture about hiccups in time, too small intervals use too
     * much of memory and can also generate overwhelming amount of data.
     */
    public static final long AGGREGATION_INTERVAL_MILLIS = SECONDS.toMillis(5);

    /**
     * Number of buckets to be created. Jitter monitor records a floating window
     * where the length of the window can be calculated as
     * <code>AGGREGATION_INTERVAL_MILLIS * CAPACITY</code>
     *
     * It has to be a power of two.
     */
    public static final  int CAPACITY = nextPowerOfTwo(720);

    /**
     * Resolution of the measurement. Smaller number can detect shorter pauses,
     * but it can cause too much of overhead. Too long value causes less overhead,
     * but it may miss shorter pauses.
     */
    public static final long RESOLUTION_NANOS = MILLISECONDS.toNanos(10);

    /**
     * Hiccups over this threshold will counted separately. This is useful for counting
     * serious hiccups.
     *
     */
    public static final long LONG_HICCUP_THRESHOLD = SECONDS.toNanos(1);


    static {
        JitterMonitor.ensureRunning();
    }

    @Override
    public Statement apply(final Statement base, final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                long startTime = System.currentTimeMillis();
                try {
                    base.evaluate();
                } catch (Throwable t) {
                    long endTime = System.currentTimeMillis();
                    Iterable<Slot> slotsBetween = JitterMonitor.getSlotsBetween(startTime, endTime);
                    StringBuilder sb = new StringBuilder("Hiccups measured while running test '")
                            .append(description.getDisplayName())
                            .append(":'")
                            .append(LINE_SEPARATOR);
                    DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
                    for (Slot slot : slotsBetween) {
                        sb.append(slot.toHumanFriendly(dateFormat)).append(LINE_SEPARATOR);
                    }
                    System.out.println(sb);
                    throw t;
                }
            }
        };
    }
}
