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

package com.hazelcast.jet.datamodel;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import static com.hazelcast.jet.datamodel.ItemsByTag.itemsByTag;
import static com.hazelcast.jet.datamodel.Tag.tag;
import static com.hazelcast.jet.datamodel.Tag.tag0;
import static com.hazelcast.jet.datamodel.Tag.tag1;
import static com.hazelcast.jet.datamodel.Tag.tag2;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
public class DataModelSerializerHooksTest {

    @Parameter
    public Object instance;

    @Parameters
    public static Collection<Object> data() {
        return asList(
                new WindowResult<>(1, 2, "value", false),
                new KeyedWindowResult<>(1, 2, "key", "value", true),
                tuple2("value-0", "value-1"),
                tuple3("value-0", "value-1", "value-2"),
                tag0(),
                tag1(),
                tag2(),
                tag(0),
                tag(1),
                tag(2),
                tag(3),
                itemsByTag(tag0(), "val0",
                        tag1(), "val1",
                        tag2(), null)
        );
    }

    @Test
    public void testSerializerHook() {
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
