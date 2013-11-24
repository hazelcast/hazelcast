package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.client.ClientTestSupport;
import com.hazelcast.client.SimpleClient;
import com.hazelcast.concurrent.atomicreference.client.*;
import com.hazelcast.config.Config;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
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
    public void setAndGet() throws Exception {
        IAtomicReference<String> reference = getAtomicReference();

        final SimpleClient client = getClient();

        client.send(new SetAndGetRequest(name, toData(null)));
        assertNull(client.receive());
        assertNull(reference.get());

        client.send(new SetAndGetRequest(name, toData("foo")));
        assertEquals("foo", client.receive());
        assertEquals("foo",reference.get());

        client.send(new SetAndGetRequest(name, toData("foo")));
        assertEquals("foo", client.receive());
        assertEquals("foo",reference.get());

        client.send(new SetAndGetRequest(name, toData("bar")));
        assertEquals("bar", client.receive());
        assertEquals("bar",reference.get());

        client.send(new SetAndGetRequest(name, toData(null)));
        assertNull(client.receive());
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
        assertEquals("foo",reference.get());

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