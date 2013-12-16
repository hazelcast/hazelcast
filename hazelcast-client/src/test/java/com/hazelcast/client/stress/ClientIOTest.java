package com.hazelcast.client.stress;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.helpers.PortableHelpersFactory;
import com.hazelcast.client.helpers.SimpleClientInterceptor;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by danny on 12/16/13.
 */
public class ClientIOTest {


    static HazelcastInstance server1;
    static HazelcastInstance server2;
    static HazelcastInstance server3;

    static HazelcastInstance client;

    static SimpleClientInterceptor interceptor;


    @BeforeClass
    public static void init() {

        Config config = new Config();

        NearCacheConfig cash = new NearCacheConfig();
        cash.setMaxSize(Integer.MAX_VALUE);
        config.getMapConfig("defaulf").setNearCacheConfig(cash);

        config.getSerializationConfig().addPortableFactory(PortableHelpersFactory.ID, new PortableHelpersFactory());
        server1 = Hazelcast.newHazelcastInstance(config);
        server2 = Hazelcast.newHazelcastInstance(config);
        server3 = Hazelcast.newHazelcastInstance(config);


        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().addPortableFactory(PortableHelpersFactory.ID, new PortableHelpersFactory());
        client = HazelcastClient.newHazelcastClient(clientConfig);



        interceptor = new SimpleClientInterceptor();
    }

    @AfterClass
    public static void destroy() {
        client.getLifecycleService().shutdown();
        Hazelcast.shutdownAll();
    }

    @Test
    public void clientIO() throws InterruptedException {

        final IMap<Object, Object> map = client.getMap("map");

        String id = map.addInterceptor(interceptor);


        map.addLocalEntryListener(new MyListner());
        map.addLocalEntryListener(new MyListner());
        map.addLocalEntryListener(new MyListner());


        map.addEntryListener(new MyListner(), true);
        map.addEntryListener(new MyListner(), true);
        map.addEntryListener(new MyListner(), true);
        map.addEntryListener(new MyListner(), true);
        map.addEntryListener(new MyListner(), true);
        map.addEntryListener(new MyListner(), true);

        map.put(1, "New York");

        map.removeAsync();


        CyclicBarrier gate = new CyclicBarrier(3);
        startGatedThread(new GatedThread(gate) {
            public void go() {
                createDataIn(clusterA, "map", 0, 1000);
            }
        });
        startGatedThread(new GatedThread(gate) {
            public void go() {
                createDataIn(clusterB, "map", 500, 1500);
            }
        });
        gate.await();
    }


    public class MyListner implements ItemListener, EntryListener {

        public void entryAdded(EntryEvent event) {
            System.out.println("Entry added key=" + event.getKey() + ", value=" + event.getValue());
        }

        public void entryRemoved(EntryEvent event) {
            System.out.println("Entry removed key=" + event.getKey() + ", value=" + event.getValue());
        }

        public void entryUpdated(EntryEvent event) {
            System.out.println("Entry update key=" + event.getKey() + ", value=" + event.getValue());
        }

        public void entryEvicted(EntryEvent event) {
            System.out.println("Entry evicted key=" + event.getKey() + ", value=" + event.getValue());
        }

        public void itemAdded(ItemEvent item) {
            System.out.println("Item added = " + item);
        }

        public void itemRemoved(ItemEvent item) {
            System.out.println("Item removed = " + item);
        }
    }

}
