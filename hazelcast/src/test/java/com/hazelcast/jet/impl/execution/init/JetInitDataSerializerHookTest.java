/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.impl.JobRecord;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.reflect.Modifier.FINAL;
import static java.lang.reflect.Modifier.STATIC;
import static java.lang.reflect.Modifier.TRANSIENT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
public class JetInitDataSerializerHookTest {

    @Parameter
    public String name;

    @Parameter(1)
    public Object instance;

    @Parameter(2)
    public Collection<String> ignoredFields;

    @Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return asList(
                new Object[]{
                        "JobRecord",
                        new JobRecord(Version.of(2, 4), 1, new HeapData(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9}),
                                "dagJson", new JobConfig(), Collections.emptySet(), null),
                        singleton("config")},

                new Object[]{
                        "JobExecutionRecord",
                        populateFields(new JobExecutionRecord(), Arrays.asList("snapshotStats", "suspensionCause")),
                        emptyList()},

                new Object[]{
                        "JobExecutionRecord.SnapshotStats",
                        populateFields(new JobExecutionRecord.SnapshotStats(), emptyList()),
                        emptyList()}
        );
    }

    @Test
    public void testSerializerHook() throws Exception {
        assertFalse("Type implements java.io.Serializable", instance instanceof Serializable);
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data serialized = serializationService.toData(instance);
        Object deserialized = serializationService.toObject(serialized);
        assertNotSame("serialization/deserialization didn't take place", instance, deserialized);

        //compare all field using reflection
        compareFields(instance, deserialized, ignoredFields);
    }

    private static Object populateFields(Object object, Collection<String> ignoredFields) {
        return populateFields2(object, object.getClass(), ignoredFields);
    }

    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
    private static Object populateFields2(Object object, Class<?> clazz, Collection<String> ignoredFields) {
        byte i = 1;
        for (Field field : clazz.getDeclaredFields()) {
            int modifiers = field.getModifiers();
            if (ignoredFields.contains(field.getName()) || (modifiers & (TRANSIENT | STATIC)) != 0) {
                continue;
            }
            field.setAccessible(true);
            Class<?> type = field.getType();
            try {
                if ((modifiers & FINAL) != 0) {
                    if (type == AtomicInteger.class) {
                        AtomicInteger val = (AtomicInteger) field.get(object);
                        val.set(i);
                    } else if (type == AtomicLong.class) {
                        AtomicLong val = (AtomicLong) field.get(object);
                        val.set(i);
                    } else {
                        throw new UnsupportedOperationException("Unsupported type: " + type);
                    }
                } else {
                    Object val;
                    if (type == String.class) {
                        val = String.valueOf(i);
                    } else if (type == int.class || type == long.class || type == byte.class || type == short.class
                            || type == float.class || type == double.class) {
                        val = i;
                    } else if (type == boolean.class) {
                        val = true;
                    } else {
                        throw new UnsupportedOperationException("Unsupported type: " + type);
                    }
                    field.set(object, val);
                }
            } catch (Exception e) {
                throw new RuntimeException("Error when setting value for '" + clazz.getSimpleName() + '.' + field.getName()
                        + "': " + e);
            }
            i++;
        }
        if (clazz.getSuperclass() != null) {
            populateFields2(object, clazz.getSuperclass(), ignoredFields);
        }
        return object;
    }

    private static void compareFields(Object original, Object cloned, Collection<String> ignoredFields) throws Exception {
        compareFields2(original, cloned, original.getClass(), ignoredFields);
    }

    private static void compareFields2(Object original, Object cloned, Class<?> clazz, Collection<String> ignoredFields)
            throws Exception {
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            Object valueOriginal = field.get(original);
            Object valueCloned = field.get(cloned);
            if (ignoredFields.contains(field.getName()) || (field.getModifiers() & TRANSIENT) != 0) {
                assertTrue("Field '" + field.getName() + "': both values should be null or not null, but are original="
                        + valueOriginal + ", cloned=" + valueCloned,
                        valueOriginal == null == (valueCloned == null));
                continue;
            }
            customAssertEquals("Field '" + field.getName() + "' not equal", valueOriginal, valueCloned);
        }
        if (clazz.getSuperclass() != null) {
            compareFields2(original, cloned, clazz.getSuperclass(), ignoredFields);
        }
    }

    private static void customAssertEquals(String msg, Object expected, Object actual) {
        if (expected instanceof AtomicLong) {
            assertEquals(msg, ((AtomicLong) expected).get(), ((AtomicLong) actual).get());
        } else if (expected instanceof AtomicInteger) {
            assertEquals(msg, ((AtomicInteger) expected).get(), ((AtomicInteger) actual).get());
        } else {
            assertEquals(msg, expected, actual);
        }
    }
}
