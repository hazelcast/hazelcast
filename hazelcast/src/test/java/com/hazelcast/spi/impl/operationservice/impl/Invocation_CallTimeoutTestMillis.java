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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.spi.impl.operationservice.OperationAccessor.setCallTimeout;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_CallTimeoutTestMillis extends HazelcastTestSupport {

    private static final long CALL_TIMEOUT = 12345;

    private HazelcastInstance hz;
    private OperationServiceImpl opService;
    private Address thisAddress;

    @Before
    public void setup() {
        Config config = new Config();
        config.setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "" + CALL_TIMEOUT);

        hz = createHazelcastInstance(config);
        opService = getOperationService(hz);
        thisAddress = getAddress(hz);
    }

    @Test
    public void callTimeout_whenDefaults() {
        Operation op = new DummyOperation();
        InvocationFuture future = (InvocationFuture) opService.invokeOnTarget(null, op, thisAddress);

        assertEquals(CALL_TIMEOUT, future.invocation.callTimeoutMillis);
        assertEquals(CALL_TIMEOUT, future.invocation.op.getCallTimeout());
    }

    @Ignore
    @Test
    public void callTimeout_whenExplicitlySet() {
        Operation op = new DummyOperation();

        int explicitCallTimeout = 12;
        setCallTimeout(op, explicitCallTimeout);

        InvocationFuture future = (InvocationFuture) opService.invokeOnTarget(null, op, thisAddress);

        assertEquals(explicitCallTimeout, future.invocation.callTimeoutMillis);
        assertEquals(explicitCallTimeout, future.invocation.op.getCallTimeout());
    }

    @Test
    public void callTimeout_whenExplicitlySet_andUsingBuilder() {
        Operation op = new DummyOperation();
        int explicitCallTimeout = 12;

        InvocationFuture future = (InvocationFuture) opService.createInvocationBuilder(null, op, thisAddress)
                .setCallTimeout(explicitCallTimeout)
                .invoke();

        assertEquals(explicitCallTimeout, future.invocation.callTimeoutMillis);
        assertEquals(explicitCallTimeout, future.invocation.op.getCallTimeout());
    }
}
