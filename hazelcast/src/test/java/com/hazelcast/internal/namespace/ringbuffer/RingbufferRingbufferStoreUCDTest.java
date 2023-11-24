/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.namespace.ringbuffer;

import com.hazelcast.config.RingbufferStoreConfig;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

public class RingbufferRingbufferStoreUCDTest extends RingbufferUCDTest {
    @Override
    public void test() throws Exception {
        assertEquals(Long.MAX_VALUE, ringBuffer.tailSequence());
    }

    @Override
    protected String getUserDefinedClassName() {
        return "usercodedeployment.LargeSequenceRingBufferStore";
    }

    @Override
    protected void addClassInstanceToConfig() throws ReflectiveOperationException {
        ringBufferConfig.setRingbufferStoreConfig(new RingbufferStoreConfig().setStoreImplementation(getClassInstance()));
    }

    @Override
    protected void addClassNameToConfig() {
        ringBufferConfig.setRingbufferStoreConfig(new RingbufferStoreConfig().setClassName(getUserDefinedClassName()));
    }

    @Parameterized.Parameters(name = "Connection: {0}, Config: {1}, Class Registration: {2}, Assertion: {3}")
    public static Iterable<Object[]> parameters() {
        return listenerParametersWithoutInstanceInDataStructure();
    }
}
