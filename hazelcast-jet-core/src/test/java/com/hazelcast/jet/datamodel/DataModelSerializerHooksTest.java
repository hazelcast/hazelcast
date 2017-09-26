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

package com.hazelcast.jet.datamodel;

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.core.TimestampedEntry;
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
import java.util.Collection;
import java.util.Map;

import static com.hazelcast.jet.datamodel.Tag.tag0;
import static com.hazelcast.jet.datamodel.Tag.tag1;
import static com.hazelcast.jet.datamodel.Tag.tag2;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;

@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelTest.class})
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class DataModelSerializerHooksTest {

    @Parameter
    public Object instance;

    @Parameters
    public static Collection<Object> data() throws Exception {
        return asList(
                new TimestampedEntry<>(1, "key", "value"),
                new Tuple2<>("value-0", "value-1"),
                new Tuple3<>("value-0", "value-1", "value-2"),
                new TwoBags<>(asList("v1", "v2"), asList("v3", "v4")),
                new ThreeBags<>(asList("v1", "v2"), asList("v3", "v4"), asList("v5", "v6")),
                tag0(),
                tag1(),
                tag2(),
                Tag.tag(0),
                Tag.tag(1),
                Tag.tag(2),
                Tag.tag(3),
                newItemsByTag(),
                newBagsByTag()
        );
    }

    private static ItemsByTag newItemsByTag() {
        ItemsByTag ibt = new ItemsByTag();
        ibt.put(tag0(), "val0");
        ibt.put(tag1(), "val1");
        ibt.put(tag2(), null);
        return ibt;
    }

    private static BagsByTag newBagsByTag() {
        BagsByTag bbt = new BagsByTag();
        bbt.ensureBag(tag0()).add("bagv0");
        bbt.ensureBag(tag0()).add("bagv1");
        bbt.ensureBag(tag1()).add("bagv2");
        bbt.ensureBag(tag1()).add("bagv3");
        return bbt;
    }

    @Test
    public void testSerializerHook() throws Exception {
        if (!(instance instanceof Map.Entry || instance instanceof Tag)) {
            assertFalse(instance.getClass() + " implements java.io.Serializable", instance instanceof Serializable);
        }
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data serialized = serializationService.toData(instance);
        Object deserialized = serializationService.toObject(serialized);
        if (!(instance instanceof Tag)) {
            assertNotSame("serialization/deserialization didn't take place", instance, deserialized);
        }
        assertEquals("objects are not equal after serialize/deserialize", instance, deserialized);
    }
}
