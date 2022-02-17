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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.impl.spi.impl.ClientServiceNotFoundException;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.exception.ServiceNotFoundException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test ClientICacheManager.getCache exception handling
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientICacheManagerTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void getCache_when_hazelcastExceptionIsThrown_then_isRethrown() {
        // when a HazelcastException occurs whose cause is not a ServiceNotFoundException
        HazelcastInstance hzInstance = mock(HazelcastInstance.class);
        when(hzInstance.getDistributedObject(anyString(), anyString())).thenThrow(new HazelcastException("mock exception"));

        ClientICacheManager clientCacheManager = new ClientICacheManager(hzInstance);
        // then the exception is rethrown
        thrown.expect(HazelcastException.class);
        clientCacheManager.getCache("any-cache");
    }

    @Test
    public void getCache_when_serviceNotFoundExceptionIsThrown_then_illegalStateExceptionIsThrown() {
        // when HazelcastException with ServiceNotFoundException cause was thrown by hzInstance.getDistributedObject
        // (i.e. cache support is not available server-side)
        HazelcastInstance hzInstance = mock(HazelcastInstance.class);
        HazelcastException hzException = new HazelcastException("mock exception",
                new ServiceNotFoundException("mock exception"));
        when(hzInstance.getDistributedObject(anyString(), anyString())).thenThrow(hzException);

        ClientICacheManager clientCacheManager = new ClientICacheManager(hzInstance);
        // then an IllegalStateException will be thrown by getCache
        thrown.expect(IllegalStateException.class);
        clientCacheManager.getCache("any-cache");
    }

    @Test
    public void getCache_when_clientServiceNotFoundExceptionIsThrown_then_illegalStateExceptionIsThrown() {
        // when ClientServiceNotFoundException was thrown by hzInstance.getDistributedObject
        // (i.e. cache support is not available on the client-side)
        HazelcastInstance hzInstance = mock(HazelcastInstance.class);
        when(hzInstance.getDistributedObject(anyString(), anyString()))
                .thenThrow(new ClientServiceNotFoundException("mock exception"));

        ClientICacheManager clientCacheManager = new ClientICacheManager(hzInstance);
        // then an IllegalStateException will be thrown by getCache
        thrown.expect(IllegalStateException.class);
        clientCacheManager.getCache("any-cache");
    }

}
