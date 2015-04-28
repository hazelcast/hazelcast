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
import java.lang.management.ThreadMXBean;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ThreadSensorPackTest extends HazelcastTestSupport {

    private static final ThreadMXBean MX_BEAN = ManagementFactory.getThreadMXBean();

    private BlackboxImpl blackbox;

    @Before
    public void setup() {
        blackbox = new BlackboxImpl(Logger.getLogger(BlackboxImpl.class));
    }

    @Test
    public void utilityConstructor(){
        assertUtilityConstructor(ThreadSensorPack.class);
    }

    @Test
    public void threadCount() {
        final Sensor sensor = blackbox.getSensor("thread.threadCount");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(MX_BEAN.getThreadCount(), sensor.readLong(), 10);
            }
        });
    }

    @Test
    public void peakThreadCount() {
        final Sensor sensor = blackbox.getSensor("thread.peakThreadCount");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(MX_BEAN.getPeakThreadCount(), sensor.readLong(), 10);
            }
        });
    }

    @Test
    public void daemonThreadCount() {
        final Sensor sensor = blackbox.getSensor("thread.daemonThreadCount");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(MX_BEAN.getDaemonThreadCount(), sensor.readLong(), 10);
            }
        });
    }

    @Test
    public void totalStartedThreadCount() {
        final Sensor sensor = blackbox.getSensor("thread.totalStartedThreadCount");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(MX_BEAN.getTotalStartedThreadCount(), sensor.readLong(), 10);
            }
        });
    }
}
