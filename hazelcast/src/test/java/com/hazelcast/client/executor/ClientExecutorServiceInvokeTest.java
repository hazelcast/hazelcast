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
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.client.test.executor.tasks.AppendCallable;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientExecutorServiceInvokeTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance client;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup()
            throws IOException {
        Config config = new XmlConfigBuilder(getClass().getClassLoader().getResourceAsStream("hazelcast-test-executor.xml"))
                .build();
        ClientConfig clientConfig = new XmlClientConfigBuilder("classpath:hazelcast-client-test-executor.xml").build();

        hazelcastFactory.newHazelcastInstance(config);
        client = hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testInvokeAll()
            throws Throwable {
        IExecutorService service = client.getExecutorService(randomString());
        String msg = randomString();
        Collection<Callable<String>> collection = new ArrayList<Callable<String>>();
        collection.add(new AppendCallable(msg));
        collection.add(new AppendCallable(msg));

        List<Future<String>> results = service.invokeAll(collection);
        for (Future<String> result : results) {
            assertEquals(msg + AppendCallable.APPENDAGE, result.get());
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokeAll_withTimeOut()
            throws Throwable {
        IExecutorService service = client.getExecutorService(randomString());
        Collection<Callable<String>> collection = new ArrayList<Callable<String>>();
        collection.add(new AppendCallable());
        collection.add(new AppendCallable());

        service.invokeAll(collection, 1, TimeUnit.MINUTES);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokeAny()
            throws Throwable {
        IExecutorService service = client.getExecutorService(randomString());
        Collection<Callable<String>> collection = new ArrayList<Callable<String>>();
        collection.add(new AppendCallable());
        collection.add(new AppendCallable());

        service.invokeAny(collection);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testInvokeAnyTimeOut()
            throws Throwable {
        IExecutorService service = client.getExecutorService(randomString());
        Collection<Callable<String>> collection = new ArrayList<Callable<String>>();
        collection.add(new AppendCallable());
        collection.add(new AppendCallable());
        service.invokeAny(collection, 1, TimeUnit.MINUTES);
    }
}
