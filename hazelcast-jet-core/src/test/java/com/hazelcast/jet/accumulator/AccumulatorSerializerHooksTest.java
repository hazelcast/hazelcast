/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.accumulator;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;

@RunWith(Parameterized.class)
@Category(ParallelJVMTest.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
public class AccumulatorSerializerHooksTest {

    @Parameter
    public Object instance;

    @Parameters
    public static Collection<Object> data() {
        return Arrays.asList(
                new LongAccumulator(2),
                new DoubleAccumulator(3),
                new MutableReference<>("foo"),
                new LinTrendAccumulator(7,
                        BigInteger.valueOf(9), BigInteger.valueOf(11), BigInteger.valueOf(13), BigInteger.valueOf(15)),
                new LongLongAccumulator(2, 3),
                new LongDoubleAccumulator(3, 4.5)
        );
    }

    @Test
    public void testSerializerHook() {
        assertFalse("Type implements java.io.Serializable", instance instanceof Serializable);
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data serialized = serializationService.toData(instance);
        Object deserialized = serializationService.toObject(serialized);
        assertNotSame("serialization/deserialization didn't take place", instance, deserialized);
        assertEquals("objects are not equal after serialize/deserialize", instance, deserialized);
    }
}
