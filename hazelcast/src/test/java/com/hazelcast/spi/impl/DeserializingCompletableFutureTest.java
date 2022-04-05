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

package com.hazelcast.spi.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
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
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DeserializingCompletableFutureTest {

    @Parameters
    public static Collection<Object> parameters() {
        return asList(new Object[]{false, true});
    }

    @Parameter
    public boolean deserialize;

    private final InternalSerializationService serializationService
            = new DefaultSerializationServiceBuilder().build();

    @Test
    public void test_get_Object() throws Exception {
        Object value = "value";
        DeserializingCompletableFuture future = new DeserializingCompletableFuture(serializationService, deserialize);

        future.complete(value);
        assertEquals(value, future.get());
    }

    @Test
    public void test_get_Data() throws Exception {
        Object value = "value";
        DeserializingCompletableFuture future = new DeserializingCompletableFuture(serializationService, deserialize);

        future.complete(serializationService.toData(value));

        if (deserialize) {
            assertEquals(value, future.get());
        } else {
            assertEquals(serializationService.toData(value), future.get());
        }
    }

    @Test
    public void test_get_Object_withTimeout() throws Exception {
        Object value = "value";
        DeserializingCompletableFuture future = new DeserializingCompletableFuture(serializationService, deserialize);

        future.complete(value);
        assertEquals(value, future.get(1, TimeUnit.MILLISECONDS));
    }

    @Test
    public void test_get_Data_withTimeout() throws Exception {
        Object value = "value";
        DeserializingCompletableFuture future = new DeserializingCompletableFuture(serializationService, deserialize);

        future.complete(serializationService.toData(value));

        if (deserialize) {
            assertEquals(value, future.get(1, TimeUnit.MILLISECONDS));
        } else {
            assertEquals(serializationService.toData(value), future.get());
        }
    }

    @Test
    public void test_getNow_Object() throws Exception {
        Object value = "value";
        DeserializingCompletableFuture future = new DeserializingCompletableFuture(serializationService, deserialize);

        future.complete(value);
        assertEquals(value, future.getNow("default"));
    }

    @Test
    public void test_getNow_Data() throws Exception {
        Object value = "value";
        DeserializingCompletableFuture future = new DeserializingCompletableFuture(serializationService, deserialize);

        future.complete(serializationService.toData(value));

        if (deserialize) {
            assertEquals(value, future.getNow("default"));
        } else {
            assertEquals(serializationService.toData(value), future.getNow("default"));
        }
    }

    @Test
    public void test_joinInternal() throws Exception {
        Object value = "value";
        DeserializingCompletableFuture future = new DeserializingCompletableFuture(serializationService, deserialize);

        future.complete(value);
        assertEquals(value, future.joinInternal());
    }

}
