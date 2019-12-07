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

package com.hazelcast.jet.config;

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

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Category(ParallelJVMTest.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
public class ConfigDataSerializerHookTest {

    @Parameter
    public Object instance;

    @Parameters
    public static Collection<Object> data() {
        return Arrays.asList(
                new JobConfig()
                        .setName("test")
                        .setAutoScaling(true)
                        .addJar("test.jar")
                        .setInitialSnapshotName("init-snapshot-name")
                        .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                        .setSplitBrainProtection(false)
        );
    }

    @Test
    public void testSerializerHook() {
        SerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        Data serialized = serializationService.toData(instance);
        Object deserialized = serializationService.toObject(serialized);

        assertEquals("objects are not equal after serialize/deserialize", instance, deserialized);
    }
}
