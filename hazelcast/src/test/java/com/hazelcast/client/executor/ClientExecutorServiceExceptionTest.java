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

package com.hazelcast.client.executor;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.spi.exception.RetryableIOException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientExecutorServiceExceptionTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }


    @Test(expected = TargetNotMemberException.class)
    public void testSubmitToNonMember() throws Throwable {
        hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        IExecutorService executorService = client.getExecutorService("test");

        HazelcastInstance instance2 = hazelcastFactory.newHazelcastInstance();
        Member member2 = instance2.getCluster().getLocalMember();

        instance2.shutdown();

        try {
            executorService.submitToMember((Serializable & Callable<String>) () -> "test", member2).get();
        } catch (Exception e) {
            throw e.getCause();
        }
    }

    public static class SecondTimeSuccessCallable implements Serializable, Callable {
        private static AtomicInteger runCount = new AtomicInteger();


        @Override
        public Object call() throws Exception {
            if (runCount.incrementAndGet() == 1) {
                throw new RetryableIOException();
            }
            return "SUCCESS";
        }
    }

    @Test
    public void testRetriableIOException() throws Throwable {
        hazelcastFactory.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        IExecutorService executorService = client.getExecutorService("test");

        assertEquals("SUCCESS", executorService.submit(new SecondTimeSuccessCallable()).get());
    }
}
