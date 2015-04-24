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

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClassLoadingSensorPackTest extends HazelcastTestSupport {

    private static final ClassLoadingMXBean BEAN = ManagementFactory.getClassLoadingMXBean();

    private BlackboxImpl blackbox;

    @Before
    public void setup() {
        blackbox = new BlackboxImpl(Logger.getLogger(BlackboxImpl.class));
    }

    @Test
    public void utilityConstructor(){
        assertUtilityConstructor(ClassLoadingSensorPack.class);
    }

    @Test
    public void loadedClassesCount() {
        final Sensor sensor = blackbox.getSensor("classloading.loadedClassesCount");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(BEAN.getLoadedClassCount(), sensor.readLong(), 100);
            }
        });
    }

    @Test
    public void totalLoadedClassesCount() {
        final Sensor sensor = blackbox.getSensor("classloading.totalLoadedClassesCount");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(BEAN.getTotalLoadedClassCount(), sensor.readLong(), 100);
            }
        });
    }

    @Test
    public void unloadedClassCount() {
        final Sensor sensor = blackbox.getSensor("classloading.unloadedClassCount");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(BEAN.getUnloadedClassCount(), sensor.readLong(), 100);
            }
        });
    }

}
