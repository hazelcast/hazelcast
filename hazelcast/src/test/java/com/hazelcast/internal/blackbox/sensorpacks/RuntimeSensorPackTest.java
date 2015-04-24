package com.hazelcast.internal.blackbox.sensorpacks;

import com.hazelcast.internal.blackbox.Sensor;
import com.hazelcast.internal.blackbox.impl.BlackboxImpl;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RuntimeSensorPackTest extends HazelcastTestSupport {

    private final static int TEN_MB = 10 * 1024 * 1024;

    private BlackboxImpl blackbox;
    private Runtime runtime;

    @Before
    public void setup() {
        blackbox = new BlackboxImpl(Logger.getLogger(BlackboxImpl.class));
        runtime = Runtime.getRuntime();
    }

    @Test
    public void utilityConstructor(){
        assertUtilityConstructor(RuntimeSensorPack.class);
    }

    @Test
    public void freeMemory() {
        final Sensor sensor = blackbox.getSensor("runtime.freeMemory");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(runtime.freeMemory(), sensor.readLong(), TEN_MB);
            }
        });
    }

    @Test
    public void totalMemory() {
        final Sensor sensor = blackbox.getSensor("runtime.totalMemory");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(runtime.totalMemory(), sensor.readLong(), TEN_MB);
            }
        });
    }

    @Test
    public void maxMemory() {
        final Sensor sensor = blackbox.getSensor("runtime.maxMemory");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(runtime.maxMemory(), sensor.readLong(), TEN_MB);
            }
        });
    }

    @Test
    public void usedMemory() {
        final Sensor sensor = blackbox.getSensor("runtime.usedMemory");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                double expected = runtime.totalMemory() - runtime.freeMemory();
                assertEquals(expected, sensor.readLong(), TEN_MB);
            }
        });
    }

    @Test
    public void availableProcessors() {
        Sensor sensor = blackbox.getSensor("runtime.availableProcessors");
        assertEquals(runtime.availableProcessors(), sensor.readLong());
    }

    @Test
    public void uptime() {
        final Sensor sensor = blackbox.getSensor("runtime.uptime");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                double expected = ManagementFactory.getRuntimeMXBean().getUptime();
                assertEquals(expected, sensor.readLong(), TimeUnit.MINUTES.toMillis(1));
            }
        });
    }
}
