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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static java.util.Arrays.asList;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DeserializingCompletableFutureTest_CompletionStageTest
        extends InternalCompletableFutureTest {

    @Parameters
    public static Collection<Object> parameters() {
        return asList(new Object[]{false, true});
    }

    @Parameter
    public boolean deserialize;

    private InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
    private Data returnValueAsData;

    @Before
    public void setup() {
        returnValueAsData = serializationService.toData(returnValue);
    }

    @Override
    protected InternalCompletableFuture<Object> incompleteFuture() {
        return deserialize ? new DeserializingCompletableFuture<>(serializationService, deserialize)
                    : new DeserializingCompletableFuture<>();
    }

    @Override
    protected Runnable completeNormally(InternalCompletableFuture<Object> future) {
        return () -> {
            if (deserialize) {
                future.complete(returnValueAsData);
            } else {
                future.complete(returnValue);
            }
        };
    }

}
