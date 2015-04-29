package com.hazelcast.internal.blackbox.impl;

import com.hazelcast.internal.blackbox.DoubleSensorInput;
import com.hazelcast.internal.blackbox.LongSensorInput;
import com.hazelcast.internal.blackbox.Sensor;
import com.hazelcast.internal.blackbox.SensorInput;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.lang.Math.round;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SensorImplTest {

    private BlackboxImpl blackbox;

    @Before
    public void setup() {
        blackbox = new BlackboxImpl(Logger.getLogger(BlackboxImpl.class));
    }

    class SomeObject {
        @SensorInput
        long longField = 10;
        @SensorInput
        double doubleField = 10.8;
    }

    @Test
    public void getParameter() {
        Sensor sensor = blackbox.getSensor("foo");

        String actual = sensor.getParameter();

        assertEquals("foo", actual);
    }

    //  ============ getLong ===========================

    @Test
    public void readLong_whenNoSensorInput() {
        Sensor sensor = blackbox.getSensor("foo");

        long actual = sensor.readLong();

        assertEquals(0, actual);
    }

    @Test
    public void readLong_whenDoubleSensorFunction() {
        blackbox.register(this, "foo", new DoubleSensorInput<SensorImplTest>() {
            @Override
            public double get(SensorImplTest source) throws Exception {
                return 10;
            }
        });

        Sensor sensor = blackbox.getSensor("foo");

        long actual = sensor.readLong();

        assertEquals(10, actual);
    }

    @Test
    public void readLong_whenLongSensorFunction() {
        blackbox.register(this, "foo", new LongSensorInput() {
            @Override
            public long get(Object o) throws Exception {
                return 10;
            }
        });

        Sensor sensor = blackbox.getSensor("foo");
        assertEquals(10, sensor.readLong());
    }

    @Test
    public void readLong_whenExceptionalInput() {
        blackbox.register(this, "foo", new LongSensorInput() {
            @Override
            public long get(Object o) {
                throw new RuntimeException();
            }
        });

        Sensor sensor = blackbox.getSensor("foo");

        long actual = sensor.readLong();

        assertEquals(0, actual);
    }

    @Test
    public void readLong_whenLongSensorField() {
        SomeObject someSensor = new SomeObject();
        blackbox.scanAndRegister(someSensor, "foo");

        Sensor sensor = blackbox.getSensor("foo.longField");
        assertEquals(10, sensor.readLong());
    }

    @Test
    public void readLong_whenDoubleSensorField() {
        SomeObject someSensor = new SomeObject();
        blackbox.scanAndRegister(someSensor, "foo");

        Sensor sensor = blackbox.getSensor("foo.doubleField");
        assertEquals(round(someSensor.doubleField), sensor.readLong());
    }

    // ============ readDouble ===========================

    @Test
    public void readDouble_whenNoSensorInput() {
        Sensor sensor = blackbox.getSensor("foo");

        double actual = sensor.readDouble();

        assertEquals(0, actual, 0.1);
    }

    @Test
    public void readDouble_whenExceptionalInput() {
        blackbox.register(this, "foo", new DoubleSensorInput() {
            @Override
            public double get(Object o) {
                throw new RuntimeException();
            }
        });

        Sensor sensor = blackbox.getSensor("foo");

        double actual = sensor.readDouble();

        assertEquals(0, actual, 0.1);
    }

    @Test
    public void readDouble_whenDoubleSensorInput() {
        blackbox.register(this, "foo", new DoubleSensorInput() {
            @Override
            public double get(Object o) {
                return 10;
            }
        });
        Sensor sensor = blackbox.getSensor("foo");

        double actual = sensor.readDouble();

        assertEquals(10, actual, 0.1);
    }

    @Test
    public void readDouble_whenLongSensorInput() {
        blackbox.register(this, "foo", new LongSensorInput() {
            @Override
            public long get(Object o) throws Exception {
                return 10;
            }
        });
        Sensor sensor = blackbox.getSensor("foo");

        double actual = sensor.readDouble();

        assertEquals(10, actual, 0.1);
    }

    @Test
    public void readDouble_whenLongSensorField() {
        SomeObject someSensor = new SomeObject();
        blackbox.scanAndRegister(someSensor, "foo");

        Sensor sensor = blackbox.getSensor("foo.longField");
        assertEquals(someSensor.longField, sensor.readDouble(), 0.1);
    }

    @Test
    public void readDouble_whenDoubleSensorField() {
        SomeObject someSensor = new SomeObject();
        blackbox.scanAndRegister(someSensor, "foo");

        Sensor sensor = blackbox.getSensor("foo.doubleField");
        assertEquals(someSensor.doubleField, sensor.readDouble(), 0.1);
    }

    // ====================== render ===================================

    @Test
    public void render_whenDoubleSensorInput() {
        blackbox.register(this, "foo", new DoubleSensorInput() {
            @Override
            public double get(Object o) {
                return 10;
            }
        });
        Sensor sensor = blackbox.getSensor("foo");
        StringBuilder sb = new StringBuilder();

        sensor.render(sb);

        assertEquals("foo=10.0", sb.toString());
    }

    @Test
    public void render_whenLongSensorFunction() {
        blackbox.register(this, "foo", new LongSensorInput() {
            @Override
            public long get(Object o) {
                return 10;
            }
        });
        Sensor sensor = blackbox.getSensor("foo");
        StringBuilder sb = new StringBuilder();

        sensor.render(sb);

        assertEquals("foo=10", sb.toString());
    }

    @Test
    public void render_whenNoSensorInput() {
        Sensor sensor = blackbox.getSensor("foo");
        StringBuilder sb = new StringBuilder();

        sensor.render(sb);

        assertEquals("foo=NA", sb.toString());
    }

    @Test
    public void render_whenNoSource() {
        Sensor sensor = blackbox.getSensor("foo");
        StringBuilder sb = new StringBuilder();

        sensor.render(sb);

        assertEquals("foo=NA", sb.toString());
    }

    @Test
    public void render_whenDoubleField() {
        SomeObject someSensor = new SomeObject();
        blackbox.scanAndRegister(someSensor, "foo");
        Sensor sensor = blackbox.getSensor("foo.doubleField");
        StringBuilder sb = new StringBuilder();

        sensor.render(sb);
        assertEquals("foo.doubleField=10.8", sb.toString());
    }

    @Test
    public void render_whenLongField() {
        SomeObject someSensor = new SomeObject();
        blackbox.scanAndRegister(someSensor, "foo");
        Sensor sensor = blackbox.getSensor("foo.longField");
        StringBuilder sb = new StringBuilder();

        sensor.render(sb);
        assertEquals("foo.longField=10", sb.toString());
    }

    @Test
    public void render_whenException() {
        blackbox.register(this, "foo", new LongSensorInput() {
            @Override
            public long get(Object o) {
                throw new RuntimeException();
            }
        });
        Sensor sensor = blackbox.getSensor("foo");
        StringBuilder sb = new StringBuilder();

        sensor.render(sb);

        assertEquals("foo=NA", sb.toString());
    }
}
