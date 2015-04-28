package com.hazelcast.internal.blackbox.impl;

import com.hazelcast.internal.blackbox.LongSensorInput;
import com.hazelcast.internal.blackbox.Sensor;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class BlackboxImplTest extends HazelcastTestSupport {

    private BlackboxImpl blackbox;

    @Before
    public void setup() {
        blackbox = new BlackboxImpl(Logger.getLogger(BlackboxImpl.class));
    }

    @Test
    public void modCount() {
        long modCount = blackbox.modCount();
        blackbox.register(this, "foo", new LongSensorInput() {
            @Override
            public long get(Object obj) throws Exception {
                return 1;
            }
        });
        assertEquals(modCount + 1, blackbox.modCount());

        blackbox.deregister(this);
        assertEquals(modCount + 2, blackbox.modCount());
    }

    // ================ getSensor ======================

    @Test(expected = NullPointerException.class)
    public void getSensor_whenNullParameter() {
        blackbox.getSensor(null);
    }

    @Test
    public void getSensor_whenNoneExistingSensor() {
        Sensor sensor = blackbox.getSensor("foo");
        assertNotNull(sensor);
        assertEquals("foo", sensor.getParameter());
        assertEquals(0, sensor.readLong());
    }

    @Test
    public void getSensor_whenExistingSensor() {
        Sensor first = blackbox.getSensor("foo");
        Sensor second = blackbox.getSensor("foo");

        assertSame(first, second);
    }

    @Test
    public void getSensors() {
        Set<String> expected = new HashSet<String>();
        expected.add("first");
        expected.add("second");
        expected.add("third");

        for (String parameter : expected) {
            blackbox.register(this, parameter, new LongSensorInput() {
                @Override
                public long get(Object obj) throws Exception {
                    return 0;
                }
            });
        }

        Set<String> parameters = blackbox.getParameters();
        for (String parameter : expected) {
            assertTrue(parameters.contains(parameter));
        }
    }

    @Test
    public void shutdown(){
        blackbox.shutdown();
    }
}
