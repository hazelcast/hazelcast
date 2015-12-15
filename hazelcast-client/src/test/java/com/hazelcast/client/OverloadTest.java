package com.hazelcast.client;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by alarmnummer on 12/15/15.
 */
public class OverloadTest extends HazelcastTestSupport {

    @Before
    public void setup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void test() {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        IMap map = client.getMap("foo");
        int k = 0;
        for (; ; ) {
            k++;
            long startMs = System.currentTimeMillis();
            map.putAsync("1", "2");
            long durationMs = System.currentTimeMillis() - startMs;
            if (k % 100 == 0 && durationMs > 0 ) {
                System.out.println(k + " duration: " + durationMs + " ms");
            }
        }
    }
}
