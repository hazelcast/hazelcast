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

package com.hazelcast.jet.impl.execution;

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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.execution.DoneItem.DONE_ITEM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
public class ExecutionSerializerHooksTest {

    @Parameter
    public Object instance;

    @Parameters
    public static Collection<Object> data() {
        return Arrays.asList(
                DONE_ITEM,
                new SnapshotBarrier(17L, false),
                new BroadcastEntry<>("key", "value"),
                broadcastKey("broadcast-key")
        );
    }

    @Test
    public void testSerializerHooks() {
        if (!(instance instanceof Map.Entry)) {
            assertFalse("Type implements java.io.Serializable", instance instanceof Serializable);
        }
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        Data serialized = serializationService.toData(instance);
        Object deserialized = serializationService.toObject(serialized);
        if (instance instanceof  DoneItem) {
            assertSame("deserialized instance not same for type " + instance.getClass() , instance, deserialized);
        } else {
            assertNotSame("deserialized instance same for type " + instance.getClass(), instance, deserialized);
        }
        assertEquals("objects are not equal after serialize/deserialize", instance, deserialized);
    }
}
