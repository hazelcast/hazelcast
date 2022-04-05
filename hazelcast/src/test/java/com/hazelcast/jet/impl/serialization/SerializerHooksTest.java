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

package com.hazelcast.jet.impl.serialization;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.accumulator.LinTrendAccumulator;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.accumulator.LongDoubleAccumulator;
import com.hazelcast.jet.accumulator.LongLongAccumulator;
import com.hazelcast.jet.accumulator.MutableReference;
import com.hazelcast.jet.accumulator.PickAnyAccumulator;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.impl.execution.BroadcastEntry;
import com.hazelcast.jet.impl.execution.SnapshotBarrier;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.math.BigInteger;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.datamodel.ItemsByTag.itemsByTag;
import static com.hazelcast.jet.datamodel.Tag.tag;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.datamodel.Tuple4.tuple4;
import static com.hazelcast.jet.datamodel.Tuple5.tuple5;
import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SerializerHooksTest {

    @Parameter
    public Object instance;

    @Parameters
    public static Collection<Object> data() {
        return Arrays.asList(
                new Object[]{new String[]{"a", "b", "c"}},
                new SimpleImmutableEntry<>("key", "value"),
                jetEvent(System.currentTimeMillis(), "payload"),
                new LongAccumulator(42),
                new LongLongAccumulator(42, 45),
                new LongDoubleAccumulator(42, 0.42),
                new MutableReference<>("payload"),
                new LinTrendAccumulator(42, BigInteger.ONE, BigInteger.TEN,
                        BigInteger.valueOf(42), BigInteger.valueOf(43)),
                new PickAnyAccumulator<>("picked", 42),
                tuple2("a", "b"),
                tuple3("a", "b", "c"),
                tuple4("a", "b", "c", "d"),
                tuple5("a", "b", "c", "d", "e"),
                itemsByTag(tag(1), "value"),
                new Watermark(42),
                new SnapshotBarrier(43, true),
                new BroadcastEntry<>("key", "value"),
                broadcastKey("key2"),
                new WindowResult<>(433, 423, "result"),
                new KeyedWindowResult<>(433, 423, "key", "result"),
                new TimestampedItem<>(423, "result")
        );
    }

    @Test
    public void testSerializerHooks() {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        Data serialized = serializationService.toData(instance);
        Object deserialized = serializationService.toObject(serialized);

        assertNotSame("serialization/deserialization didn't take place", instance, deserialized);
        if (instance instanceof Object[]) {
            assertArrayEquals("objects are not equal after serialize/deserialize",
                    (Object[]) instance, (Object[]) deserialized);
        } else {
            assertEquals("objects are not equal after serialize/deserialize", instance, deserialized);
        }
    }
}
