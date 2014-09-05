/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.client.ClientTestSupport;
import com.hazelcast.client.SimpleClient;
import com.hazelcast.concurrent.atomiclong.client.*;
import com.hazelcast.config.Config;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author ali 5/13/13
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class AtomicLongClientRequestTest extends ClientTestSupport {

    static final String name = "test";

    protected Config createConfig() {
        return new Config();
    }

    private IAtomicLong getAtomicLong() {
        return getInstance().getAtomicLong(name);
    }

    @Test
    public void testAddAndGet() throws Exception {
        final SimpleClient client = getClient();
        client.send(new AddAndGetRequest(name, 3));
        long result = (Long) client.receive();
        assertEquals(3, result);

        client.send(new AddAndGetRequest(name, 4));
        result = (Long) client.receive();
        assertEquals(7, result);
    }

    @Test
    public void testCompareAndSet() throws Exception {
        IAtomicLong atomicLong = getAtomicLong();
        atomicLong.set(11);

        final SimpleClient client = getClient();
        client.send(new CompareAndSetRequest(name, 9, 5));
        boolean result = (Boolean) client.receive();
        assertFalse(result);
        assertEquals(11, atomicLong.get());

        client.send(new CompareAndSetRequest(name, 11, 5));
        result = (Boolean) client.receive();
        assertTrue(result);
        assertEquals(5, atomicLong.get());
    }

    @Test
    public void testGetAndAdd() throws IOException {
        IAtomicLong atomicLong = getAtomicLong();
        atomicLong.set(11);

        final SimpleClient client = getClient();
        client.send(new GetAndAddRequest(name, 4));
        long result = (Long) client.receive();
        assertEquals(11, result);
        assertEquals(15, atomicLong.get());
    }

    @Test
    public void testGetAndSet() throws IOException {
        IAtomicLong atomicLong = getAtomicLong();
        atomicLong.set(11);

        final SimpleClient client = getClient();
        client.send(new GetAndSetRequest(name, 9));
        long result = (Long) client.receive();
        assertEquals(11, result);
        assertEquals(9, atomicLong.get());
    }

    @Test
    public void testSet() throws IOException {
        IAtomicLong atomicLong = getAtomicLong();
        atomicLong.set(11);

        final SimpleClient client = getClient();
        client.send(new SetRequest(name, 7));
        client.receive();
        assertEquals(7, atomicLong.get());
    }
}
