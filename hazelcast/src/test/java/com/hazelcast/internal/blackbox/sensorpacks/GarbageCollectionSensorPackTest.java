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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class GarbageCollectionSensorPackTest extends HazelcastTestSupport {

    private BlackboxImpl blackbox;
    private GarbageCollectionSensorPack.GcStats gcStats;

    @Before
    public void setup() {
        blackbox = new BlackboxImpl(Logger.getLogger(BlackboxImpl.class));
        gcStats = new GarbageCollectionSensorPack.GcStats();
    }

    @Test
    public void utilityConstructor(){
        assertUtilityConstructor(GarbageCollectionSensorPack.class);
    }

    @Test
    public void minorCount() {
        final Sensor sensor = blackbox.getSensor("gc.minorCount");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                gcStats.run();
                 assertEquals(gcStats.minorCount, sensor.readLong(), 1);
            }
        });
    }

    @Test
    public void minorTime() throws InterruptedException {
        final Sensor sensor = blackbox.getSensor("gc.minorTime");
         assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                gcStats.run();
                assertEquals(gcStats.minorTime, sensor.readLong(), SECONDS.toMillis(1));
            }
        });
    }

    @Test
    public void majorCount() {
        final Sensor sensor = blackbox.getSensor("gc.majorCount");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                gcStats.run();
                assertEquals(gcStats.majorCount, sensor.readLong(), 1);
            }
        });
    }

    @Test
    public void majorTime() {
        final Sensor sensor = blackbox.getSensor("gc.majorTime");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                gcStats.run();
                assertEquals(gcStats.majorTime, sensor.readLong(), SECONDS.toMillis(1));
            }
        });
    }


    @Test
    public void unknownCount() {
        final Sensor sensor = blackbox.getSensor("gc.unknownCount");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                gcStats.run();
                assertEquals(gcStats.unknownCount, sensor.readLong(), 1);
            }
        });
    }

    @Test
    public void unknownTime() {
        final Sensor sensor = blackbox.getSensor("gc.unknownTime");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                gcStats.run();
                assertEquals(gcStats.unknownTime, sensor.readLong(), SECONDS.toMillis(1));
            }
        });
    }
}
