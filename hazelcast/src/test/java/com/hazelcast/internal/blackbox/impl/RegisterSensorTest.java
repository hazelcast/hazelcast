package com.hazelcast.internal.blackbox.impl;

import com.hazelcast.internal.blackbox.SensorInput;
import com.hazelcast.internal.blackbox.Sensor;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.OutputStream;
import java.util.LinkedList;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class RegisterSensorTest extends HazelcastTestSupport {

    private BlackboxImpl blackbox;

    @Before
    public void setup() {
        blackbox = new BlackboxImpl(Logger.getLogger(BlackboxImpl.class));
    }

    @Test(expected = NullPointerException.class)
    public void whenParameterPrefixNull() {
        blackbox.scanAndRegister(new SomeField(), null);
    }

    @Test(expected = NullPointerException.class)
    public void whenObjectNull() {
        blackbox.scanAndRegister(null, "bar");
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenUnrecognizedField() {
        blackbox.scanAndRegister(new SomeUnrecognizedField(), "bar");
    }

    @Test
    public void whenNoSensorField_thenIgnore() {
        blackbox.scanAndRegister(new LinkedList(), "bar");

        for (String parameter : blackbox.getParameters()) {
            assertFalse(parameter.startsWith("bar"));
        }
    }

    public class SomeField {
        @SensorInput
        long field;
    }

    public class SomeUnrecognizedField {
        @SensorInput
        OutputStream field;
    }

    @Test
    public void deregister_whenNotRegistered() {
        MultiFieldAndMethod multiFieldAndMethod = new MultiFieldAndMethod();
        multiFieldAndMethod.field1 = 1;
        multiFieldAndMethod.field2 = 2;
        blackbox.deregister(multiFieldAndMethod);

        // make sure that the the sensors have been removed from the parameters
        Set<String> parameters = blackbox.getParameters();
        assertFalse(parameters.contains("foo.field1"));
        assertFalse(parameters.contains("foo.field2"));
        assertFalse(parameters.contains("foo.method1"));
        assertFalse(parameters.contains("foo.method2"));
 }

    public class MultiFieldAndMethod {
        @SensorInput
        long field1;
        @SensorInput
        long field2;

        @SensorInput
        int method1() {
            return 1;
        }

        @SensorInput
        int method2() {
            return 2;
        }
    }

    @Test
    public void deregister_whenRegistered() {
        MultiFieldAndMethod multiFieldAndMethod = new MultiFieldAndMethod();
        multiFieldAndMethod.field1 = 1;
        multiFieldAndMethod.field2 = 2;
        blackbox.scanAndRegister(multiFieldAndMethod, "foo");

        Sensor field1 = blackbox.getSensor("foo.field1");
        Sensor field2 = blackbox.getSensor("foo.field2");
        Sensor method1 = blackbox.getSensor("foo.method1");
        Sensor method2 = blackbox.getSensor("foo.method2");

        blackbox.deregister(multiFieldAndMethod);

        // make sure that the the sensors have been removed from the parameters
        Set<String> parameters = blackbox.getParameters();
        assertFalse(parameters.contains("foo.field1"));
        assertFalse(parameters.contains("foo.field2"));
        assertFalse(parameters.contains("foo.method1"));
        assertFalse(parameters.contains("foo.method2"));

        // make sure that the sensors have been disconnected
        assertEquals(0, field1.readLong());
        assertEquals(0, field2.readLong());
        assertEquals(0, method1.readLong());
        assertEquals(0, method2.readLong());
    }

    @Test
    public void deregister_whenAlreadyDeregistered() {
        MultiFieldAndMethod multiFieldAndMethod = new MultiFieldAndMethod();
        multiFieldAndMethod.field1 = 1;
        multiFieldAndMethod.field2 = 2;
        blackbox.scanAndRegister(multiFieldAndMethod, "foo");
        blackbox.deregister(multiFieldAndMethod);
        blackbox.deregister(multiFieldAndMethod);

        // make sure that the the sensors have been removed from the parameters
        Set<String> parameters = blackbox.getParameters();
        assertFalse(parameters.contains("foo.field1"));
        assertFalse(parameters.contains("foo.field2"));
        assertFalse(parameters.contains("foo.method1"));
        assertFalse(parameters.contains("foo.method2"));
    }
}
