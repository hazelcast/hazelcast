/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.bluegreen;

import com.hazelcast.client.impl.ClientImpl;
import com.hazelcast.client.impl.ClientSelectors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientSelectorsTest extends HazelcastTestSupport {

    @Test
    public void testAny() {
        String name = randomString();
        Set<String> labels = Collections.emptySet();
        ClientImpl client = new ClientImpl(null, createInetSocketAddress("127.0.0.1"), name, labels);
        assertTrue(client.toString(), ClientSelectors.any().select(client));
    }

    private InetSocketAddress createInetSocketAddress(String name) {
        try {
            return new InetSocketAddress(InetAddress.getByName(name), 5000);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testNone() {
        String name = randomString();
        Set<String> labels = Collections.emptySet();
        ClientImpl client = new ClientImpl(null, createInetSocketAddress("127.0.0.1"), name, labels);
        assertFalse(client.toString(), ClientSelectors.none().select(client));
    }

    @Test
    public void testLocalhostWithIp() {
        String name = randomString();
        Set<String> labels = Collections.emptySet();
        ClientImpl client = new ClientImpl(null, createInetSocketAddress("localhost"), name, labels);
        assertTrue(client.toString(), ClientSelectors.ipSelector("127.0.0.1").select(client));
    }

    @Test
    public void testNameSelector() {
        String name = "client1";
        Set<String> labels = Collections.emptySet();

        ClientImpl client = new ClientImpl(null, createInetSocketAddress("127.0.0.1"), name, labels);
        assertTrue(client.toString(), ClientSelectors.nameSelector("client1").select(client));
        assertTrue(client.toString(), ClientSelectors.nameSelector("clie.*").select(client));
        assertTrue(client.toString(), ClientSelectors.nameSelector(".*lie.*").select(client));
        assertTrue(client.toString(), ClientSelectors.nameSelector("c.*t1").select(client));

        assertFalse(client.toString(), ClientSelectors.nameSelector("client2").select(client));
        assertFalse(client.toString(), ClientSelectors.nameSelector("clii.*").select(client));
        assertFalse(client.toString(), ClientSelectors.nameSelector(".*lii.*").select(client));
        assertFalse(client.toString(), ClientSelectors.nameSelector("c.*t2").select(client));
    }

    @Test
    public void testNameSelectorsWithNullInput() {
        ClientImpl client = new ClientImpl(null, createInetSocketAddress("127.0.0.1"), null, null);
        assertFalse(client.toString(), ClientSelectors.nameSelector("client").select(client));
    }

    @Test
    public void testLabelSelector() {
        String name = randomString();
        HashSet<String> labels = new HashSet<>();
        Collections.addAll(labels, "admin", "foo", "client1");

        ClientImpl client = new ClientImpl(null, createInetSocketAddress("127.0.0.1"), name, labels);

        assertTrue(client.toString(), ClientSelectors.labelSelector("client1").select(client));
        assertTrue(client.toString(), ClientSelectors.labelSelector("clie.*").select(client));
        assertTrue(client.toString(), ClientSelectors.labelSelector(".*lie.*").select(client));
        assertTrue(client.toString(), ClientSelectors.labelSelector("c.*t1").select(client));

        assertFalse(client.toString(), ClientSelectors.labelSelector("client2").select(client));
        assertFalse(client.toString(), ClientSelectors.labelSelector("clii.*").select(client));
        assertFalse(client.toString(), ClientSelectors.labelSelector(".*lii.*").select(client));
        assertFalse(client.toString(), ClientSelectors.labelSelector("c.*t2").select(client));
    }

    @Test
    public void testIpSelector_withIpv4() {
        String name = randomString();
        Set<String> labels = Collections.emptySet();
        String ip = "213.129.127.80";
        ClientImpl client = new ClientImpl(null, createInetSocketAddress(ip), name, labels);

        assertTrue(client.toString(), ClientSelectors.ipSelector("213.129.127.80").select(client));
        assertTrue(client.toString(), ClientSelectors.ipSelector("213.129.127.*").select(client));
        assertTrue(client.toString(), ClientSelectors.ipSelector("213.129.*.1-100").select(client));
        assertFalse(client.toString(), ClientSelectors.ipSelector("213.129.127.70").select(client));
        assertFalse(client.toString(), ClientSelectors.ipSelector("213.129.126.*").select(client));
        assertFalse(client.toString(), ClientSelectors.ipSelector("213.129.*.1-10").select(client));
    }

    @Test
    public void testIpSelector_withIpv6() {
        String name = randomString();
        Set<String> labels = Collections.emptySet();
        String ip = "fe80:0:0:0:45c5:47ee:fe15:493a";
        ClientImpl client = new ClientImpl(null, createInetSocketAddress(ip), name, labels);
        assertTrue(client.toString(), ClientSelectors.ipSelector("fe80:0:0:0:45c5:47ee:fe15:493a").select(client));
        assertTrue(client.toString(), ClientSelectors.ipSelector("fe80:0:0:0:45c5:47ee:fe15:*").select(client));
        assertTrue(client.toString(), ClientSelectors.ipSelector("fe80:0:0:0:45c5:47ee:fe15:0-5555").select(client));
        assertFalse(client.toString(), ClientSelectors.ipSelector("fe80:0:0:0:45c5:47ee:fe15:493b").select(client));
        assertFalse(client.toString(), ClientSelectors.ipSelector("fe80:0:0:0:45c5:47ee:fe14:*").select(client));
        assertFalse(client.toString(), ClientSelectors.ipSelector("fe80::223:6cff:fe93:0-4444").select(client));
    }

    @Test
    public void testInverse() {
        String name = "client1";
        Set<String> labels = Collections.emptySet();

        ClientImpl client = new ClientImpl(null, createInetSocketAddress("127.0.0.1"), name, labels);
        assertFalse(client.toString(), ClientSelectors.inverse(ClientSelectors.nameSelector("client1")).select(client));
    }

    @Test
    public void testCombinationWithOr() {
        String name = "client1";
        HashSet<String> labels = new HashSet<>();
        Collections.addAll(labels, "admin", "foo", "client1");
        String ip = "213.129.127.80";
        ClientImpl client = new ClientImpl(null, createInetSocketAddress(ip), name, labels);

        assertTrue(client.toString(), ClientSelectors.or(ClientSelectors.ipSelector("213.129.*.1-100"),
                ClientSelectors.nameSelector("clie.*")).select(client));
    }
}
