package com.hazelcast.internal.serialization.impl;

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.SampleIdentifiedDataSerializable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * SerializerHookLoader Tester.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SerializerHookLoaderTest extends HazelcastTestSupport {


    private ClassLoader classLoader = getClass().getClassLoader();

    @Test
    public void testLoad_withDefaultConstructor() throws Exception {
        SerializerConfig serializerConfig = new SerializerConfig();
        serializerConfig.setClassName("com.hazelcast.internal.serialization.impl.TestSerializerHook$TestSerializer");
        serializerConfig.setTypeClassName("com.hazelcast.nio.serialization.SampleIdentifiedDataSerializable");

        SerializationConfig serializationConfig = getConfig().getSerializationConfig();
        serializationConfig.addSerializerConfig(serializerConfig);

        SerializerHookLoader hook = new SerializerHookLoader(serializationConfig, classLoader);
        Map<Class, Object> serializers = hook.getSerializers();
        assertNotNull(serializers);
    }

    @Test
    public void testLoad_withParametrizedConstructor() throws Exception {
        SerializerConfig serializerConfig = new SerializerConfig();
        serializerConfig.setClassName("com.hazelcast.internal.serialization.impl.TestSerializerHook$TestSerializerWithTypeConstructor");
        serializerConfig.setTypeClassName("com.hazelcast.nio.serialization.SampleIdentifiedDataSerializable");

        SerializationConfig serializationConfig = getConfig().getSerializationConfig();
        serializationConfig.addSerializerConfig(serializerConfig);

        SerializerHookLoader hook = new SerializerHookLoader(serializationConfig, classLoader);
        Map<Class, Object> serializers = hook.getSerializers();

        TestSerializerHook.TestSerializerWithTypeConstructor serializer = (TestSerializerHook.TestSerializerWithTypeConstructor)
                serializers.get(SampleIdentifiedDataSerializable.class);
        assertEquals(SampleIdentifiedDataSerializable.class, serializer.getClazz());
    }

    @Test
    public void testLoad_withParametrizedConstructorAndCompatibilitySwitchOn() throws Exception {
        String propName = "hazelcast.compat.serializers.use.default.constructor.only";
        String origProperty = System.getProperty(propName);
        try {
            System.setProperty(propName, "true");
            SerializerConfig serializerConfig = new SerializerConfig();
            serializerConfig.setClassName("com.hazelcast.internal.serialization.impl.TestSerializerHook$TestSerializerWithTypeConstructor");
            serializerConfig.setTypeClassName("com.hazelcast.nio.serialization.SampleIdentifiedDataSerializable");

            SerializationConfig serializationConfig = getConfig().getSerializationConfig();
            serializationConfig.addSerializerConfig(serializerConfig);

            SerializerHookLoader hook = new SerializerHookLoader(serializationConfig, classLoader);
            Map<Class, Object> serializers = hook.getSerializers();

            TestSerializerHook.TestSerializerWithTypeConstructor serializer = (TestSerializerHook.TestSerializerWithTypeConstructor)
                    serializers.get(SampleIdentifiedDataSerializable.class);
            assertNull(serializer.getClazz());
        } finally {
            if (origProperty != null) {
                System.setProperty(propName, origProperty);
            } else
                System.clearProperty(propName);
        }
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testLoad_implException() throws Exception {
        SerializerConfig serializerConfig = new SerializerConfig();
        serializerConfig.setClassName("NOT FOUND CLASS");
        serializerConfig.setTypeClassName("com.hazelcast.nio.serialization.SampleIdentifiedDataSerializable");

        SerializationConfig serializationConfig = getConfig().getSerializationConfig();
        serializationConfig.addSerializerConfig(serializerConfig);

        new SerializerHookLoader(serializationConfig, classLoader);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testLoad_typeException() throws Exception {
        SerializerConfig serializerConfig = new SerializerConfig();
        serializerConfig.setClassName("com.hazelcast.internal.serialization.impl.TestSerializerHook$TestSerializer");
        serializerConfig.setTypeClassName("NOT FOUND CLASS");

        SerializationConfig serializationConfig = getConfig().getSerializationConfig();
        serializationConfig.addSerializerConfig(serializerConfig);

        new SerializerHookLoader(serializationConfig, classLoader);
    }

} 
