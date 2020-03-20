package com.hazelcast.collection.impl.list;

import com.google.common.collect.Iterators;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BasicListTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance server;

    @Before
    public void setup() {
        Config config = getConfig();
        server = hazelcastFactory.newHazelcastInstance(config);
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testRawIterator() {
        ListProxyImpl<Integer> list = list();

        list.add(1);
        list.add(2);

        assertEquals(2, Iterators.size(list.rawIterator()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRawIterator_throwsException_whenRemove() {
        ListProxyImpl<Integer> list = list();
        list.add(1);

        Iterator<Data> iterator = list.rawIterator();

        iterator.next();
        iterator.remove();
    }

    @Test
    public void testRawSublist() {
        ListProxyImpl<Integer> list = list();
        list.add(1);
        list.add(2);
        list.add(3);

        List<Data> listTest = list.rawSubList(1, 2);

        assertEquals(1, listTest.size());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testRawSublist_whenFromIndexIllegal() {
        ListProxyImpl<Integer> list = list();

        list.subList(8, 7);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testRawSublist_whenToIndexIllegal() {
        ListProxyImpl<Integer> list = list();
        list.add(1);
        list.add(2);

        list.subList(1, 3);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T> ListProxyImpl<T> list() {
        return (ListProxyImpl) server.getList(randomName());
    }
}
