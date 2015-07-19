package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientMapEntryListenerTest {

    private static HazelcastInstance client;

    private String randomMapName = randomString();
    private IMap<String, String> randomStringMap = client.getMap(randomMapName);

    private EntryListener entryListener = mock(EntryListener.class);

    @BeforeClass
    public static void init() {
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();
    }

    @AfterClass
    public static void destroy() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListener() {
        randomStringMap.addLocalEntryListener(entryListener);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListener_WithPredicate() {
        randomStringMap.addLocalEntryListener(entryListener, new FalsePredicate(), true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListener_WithPredicateAndKey() {
        randomStringMap.addLocalEntryListener(entryListener, new FalsePredicate(), "key", true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLocalKeySet() {
        randomStringMap.localKeySet();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLocalKeySet_WithPredicate() {
        randomStringMap.localKeySet(new FalsePredicate());
    }

    private static class FalsePredicate implements Predicate<String, String> {
        @Override
        public boolean apply(Map.Entry<String, String> mapEntry) {
            return false;
        }
    }
}
