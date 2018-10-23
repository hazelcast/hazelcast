/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JobExecutionRecord;
import com.hazelcast.jet.impl.JobRecord;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Category(ParallelTest.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
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
                        new JobRecord(1, 2, new HeapData(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9}), "dagJson",
                                new JobConfig()),
                        singleton("config")},

                new Object[]{
                        "JobExecutionRecord",
                        sampleJobExecutionRecord(),
                        emptyList()}
        );
    }

    private static JobExecutionRecord sampleJobExecutionRecord() {
        JobExecutionRecord r = new JobExecutionRecord(1, 2, true);
        r.startNewSnapshot();
        r.ongoingSnapshotDone(10, 11, 12, "failure");
        return r;
    }

    @Test
    public void testSerializerHook() throws Exception {
        assertFalse("Type implements java.io.Serializable", instance instanceof Serializable);
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data serialized = serializationService.toData(instance);
        Object deserialized = serializationService.toObject(serialized);
        assertNotSame("serialization/deserialization didn't take place", instance, deserialized);

        //compare all field using reflection
        compareFields(instance, deserialized, instance.getClass(), ignoredFields);
    }

    private static void compareFields(Object original, Object cloned, Class<?> clazz, Collection<String> ignoredFields)
            throws Exception {
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            Object valueOriginal = field.get(original);
            Object valueCloned = field.get(cloned);
            if (ignoredFields.contains(field.getName())) {
                assertTrue("Field '" + field.getName() + "': both values should be null or not null, but are original="
                        + valueOriginal + ", cloned=" + valueCloned,
                        valueOriginal == null == (valueCloned == null));
                continue;
            }
            customAssertEquals("Field '" + field.getName() + "' not equal", valueOriginal, valueCloned);
        }
        if (clazz.getSuperclass() != null) {
            compareFields(original, cloned, clazz.getSuperclass(), ignoredFields);
        }
    }

    private static void customAssertEquals(String msg, Object expected, Object actual) {
        if (expected instanceof AtomicLong) {
            assertEquals(((AtomicLong) expected).get(), ((AtomicLong) actual).get());
        } else if (expected instanceof AtomicInteger) {
            assertEquals(((AtomicInteger) expected).get(), ((AtomicInteger) actual).get());
        } else {
            assertEquals(msg, expected, actual);
        }
    }
}
