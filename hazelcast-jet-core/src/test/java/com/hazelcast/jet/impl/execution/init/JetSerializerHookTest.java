/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.accumulator.DoubleAccumulator;
import com.hazelcast.jet.accumulator.LinTrendAccumulator;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.windowing.Frame;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;

@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelTest.class})
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class JetSerializerHookTest {

    @Parameter
    public Object instance;

    @Parameters
    public static Collection<Object> data() throws Exception {
        return Arrays.asList(
                new Object[]{new String[]{"a", "b", "c"}},
                new SimpleImmutableEntry<>("key", "value"),
                new Frame<>(1, "key", "value"),
                new LongAccumulator(2),
                new DoubleAccumulator(3),
                new MutableReference<>("foo"),
                new LinTrendAccumulator(7,
                        BigInteger.valueOf(9), BigInteger.valueOf(11), BigInteger.valueOf(13), BigInteger.valueOf(15))
        );
    }

    @Test
    public void testSerialization() throws Exception {
        // this test tries to serialize and deserialize all types defined in JetSerializerHook
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        Data serialized = serializationService.toData(instance);
        Object deserialized = serializationService.toObject(serialized);

        if (!(instance instanceof Map.Entry) && !(instance instanceof Object[])) {
            assertFalse("Type implements java.io.Serializable", instance instanceof Serializable);
        }
        assertNotSame("serialization/deserialization didn't take place", instance, deserialized);
        if (instance instanceof Object[]) {
            assertArrayEquals("objects are not equal after serialize/deserialize",
                    (Object[]) instance, (Object[]) deserialized);
        } else {
            assertEquals("objects are not equal after serialize/deserialize", instance, deserialized);
        }
    }
}
