package com.hazelcast.client;

import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertArrayEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class LargeValueTest extends ClientTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private IMap<String,byte[]> clientReference;
    private IMap<String,byte[]> serverReference;

    @After
    public void tearDown() throws InterruptedException {
        hazelcastFactory.terminateAll();
       // Thread.sleep(5000);
    }

    @Before
    public void setup() {
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        clientReference = client.getMap("map");
        serverReference = server.getMap("map");
    }

    @Test
    public void writingLargeValues() {
        for (int k = 1; k < 10 * 1024 * 1024; k = k * 2) {
            byte[] expected = randomByteArray(k);
            clientReference.set("key",expected);
            byte[] found = serverReference.get("key");
            assertArrayEquals(expected, found);
        }
    }

    @Test
    public void readingLargeValues() {
        for (int k = 1; k < 10 * 1024 * 1024; k = k * 2) {
            byte[] expected = randomByteArray(k);
            serverReference.set("key",expected);
            byte[] found = clientReference.get("key");
            assertArrayEquals(expected, found);
        }
    }
}
