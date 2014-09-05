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

package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.client.ClientTestSupport;
import com.hazelcast.client.SimpleClient;
import com.hazelcast.concurrent.atomicreference.client.*;
import com.hazelcast.config.Config;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ClientCompatibleTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class AtomicReferenceClientRequestTest extends ClientTestSupport {

    static final String name = "test";

    protected Config createConfig() {
        return new Config();
    }

    private IAtomicReference getAtomicReference() {
        IAtomicReference reference = getInstance().getAtomicReference(name);
        reference.set(null);
        return reference;
    }

    @Test
    public void get() throws Exception {
        IAtomicReference<String> reference = getAtomicReference();

        final SimpleClient client = getClient();
        client.send(new GetRequest(name));
        assertNull(client.receive());

        reference.set("foo");
        client.send(new GetRequest(name));
        assertEquals("foo",client.receive());
    }

    @Test
    public void isNull() throws Exception {
        IAtomicReference<String> reference = getAtomicReference();

        final SimpleClient client = getClient();
        client.send(new IsNullRequest(name));
        assertEquals(Boolean.TRUE,client.receive());

        reference.set("foo");
        client.send(new IsNullRequest(name));
        assertEquals(Boolean.FALSE,client.receive());
    }

    @Test
    @ClientCompatibleTest
    public void contains()throws Exception {
        IAtomicReference<String> reference = getAtomicReference();
        final SimpleClient client = getClient();

        client.send(new ContainsRequest(name, toData(null)));
        assertEquals(Boolean.TRUE, client.receive());

        reference.set("foo");

        client.send(new ContainsRequest(name, toData(null)));
        assertEquals(Boolean.FALSE, client.receive());

        client.send(new ContainsRequest(name, toData("foo")));
        assertEquals(Boolean.TRUE,client.receive());

        client.send(new ContainsRequest(name, toData("bar")));
        assertEquals(Boolean.FALSE,client.receive());
    }

    @Test
    public void set() throws Exception {
        IAtomicReference<String> reference = getAtomicReference();

        final SimpleClient client = getClient();

        client.send(new SetRequest(name, toData(null)));
        assertNull(client.receive());
        assertNull(reference.get());

        client.send(new SetRequest(name, toData("foo")));
        assertNull(client.receive());
        assertEquals("foo", reference.get());

        client.send(new SetRequest(name, toData("foo")));
        assertNull(client.receive());
        assertEquals("foo", reference.get());

        client.send(new SetRequest(name, toData(null)));
        assertNull(client.receive());
        assertEquals(null,reference.get());
    }

    @Test
    public void getAndSet() throws Exception {
        IAtomicReference<String> reference = getAtomicReference();

        final SimpleClient client = getClient();

        client.send(new GetAndSetRequest(name, toData(null)));
        assertNull(client.receive());
        assertNull(reference.get());

        client.send(new GetAndSetRequest(name, toData("foo")));
        assertNull(client.receive());
        assertEquals("foo",reference.get());

        client.send(new GetAndSetRequest(name, toData("foo")));
        assertEquals("foo", client.receive());
        assertEquals("foo",reference.get());

        client.send(new GetAndSetRequest(name, toData("bar")));
        assertEquals("foo", client.receive());
        assertEquals("bar",reference.get());

        client.send(new GetAndSetRequest(name, toData(null)));
        assertEquals("bar", client.receive());
        assertNull(reference.get());
    }

    @Test
    public void compareAndSet() throws Exception {
        IAtomicReference<String> reference = getAtomicReference();

        final SimpleClient client = getClient();

        client.send(new CompareAndSetRequest(name, toData(null), toData(null)));
        assertEquals(Boolean.TRUE, client.receive());
        assertNull(reference.get());

        client.send(new CompareAndSetRequest(name, toData("foo"), toData(null)));
        assertEquals(Boolean.FALSE, client.receive());
        assertNull(reference.get());

        client.send(new CompareAndSetRequest(name, toData(null), toData("foo")));
        assertEquals(Boolean.TRUE, client.receive());
        assertEquals("foo", reference.get());

        client.send(new CompareAndSetRequest(name, toData("foo"), toData("foo")));
        assertEquals(Boolean.TRUE, client.receive());
        assertEquals("foo",reference.get());

        client.send(new CompareAndSetRequest(name, toData(null), toData("pipo")));
        assertEquals(Boolean.FALSE, client.receive());
        assertEquals("foo",reference.get());

        client.send(new CompareAndSetRequest(name, toData("bar"), toData("foo")));
        assertEquals(Boolean.FALSE, client.receive());
        assertEquals("foo",reference.get());

        client.send(new CompareAndSetRequest(name, toData("foo"), toData("bar")));
        assertEquals(Boolean.TRUE, client.receive());
        assertEquals("bar",reference.get());

        client.send(new CompareAndSetRequest(name, toData("bar"), toData(null)));
        assertEquals(Boolean.TRUE, client.receive());
        assertEquals(null,reference.get());
    }

    public Data toData(Object o){
        return getNode(getInstance()).getSerializationService().toData(o);
    }
}
