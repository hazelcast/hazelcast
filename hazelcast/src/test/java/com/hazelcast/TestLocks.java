package com.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class TestLocks {

    public static final int SIZE_PER_PUT_THREAD = 2000;

    @Test
    public void test() throws InterruptedException {
        HazelcastInstance instance1 = createInstance("1", 1);
        HazelcastInstance instance2 = createInstance("1", 1);
        List<Thread> putThreads = new ArrayList<Thread>();
        IMap<Object, Object> testMapInstance1 = instance1.getMap("testMap");
        IMap<Object, Object> testMapInstance2 = instance2.getMap("testMap");
        putThreads.add(new PutThread(SIZE_PER_PUT_THREAD, testMapInstance1, instance1));
        putThreads.add(new PutThread(SIZE_PER_PUT_THREAD, testMapInstance1, instance1));
        List<Thread> takeThreads = new ArrayList<Thread>();
        CopyOnWriteArrayList<String> destination = new CopyOnWriteArrayList<String>();
        takeThreads.add(new TakeThread(testMapInstance1, destination, instance1));
        takeThreads.add(new TakeThread(testMapInstance1, destination, instance1));
        TakeThread takeThreadSecondInstance = new TakeThread(testMapInstance2, destination, instance2);
        takeThreads.add(takeThreadSecondInstance);
        for (Thread putThread : putThreads) {
            putThread.start();
        }
        for (Thread takeThread : takeThreads) {
            takeThread.start();
        }
        while (!((PutThread) putThreads.get(0)).isAlmostFinished()) {
            Thread.sleep(10l);
        }
        takeThreadSecondInstance.setStop(true);
        instance2.getLifecycleService().kill();
        while (!testMapInstance1.isEmpty()) {
            Thread.sleep(10l);
        }
        org.junit.Assert.assertThat(destination.size(), Is.is(SIZE_PER_PUT_THREAD / 4));
        while (true) {
            Thread.sleep(100);
        }
    }

    public static class PutThread extends Thread {
        private int size;
        private IMap map;
        private HazelcastInstance hazelcastInstance;
        private int index;

        public PutThread(int size, IMap map, HazelcastInstance hazelcastInstance) {
            this.size = size;
            this.map = map;
            this.hazelcastInstance = hazelcastInstance;
            this.index = 0;
        }

        @Override
        public void run() {
            for (index = 0; index < size; index++) {
                hazelcastInstance.getTransaction().begin();
                String value = UUID.randomUUID().toString();
                try {
                    System.out.println("Putting message: " + index);
                    map.put(value, value);
                    hazelcastInstance.getTransaction().commit();
                    System.out.println(Thread.currentThread().getName() + ": Commit to put " + value);
                } catch (Exception e) {
                    System.out.println("Rollbacking " + value);
                    hazelcastInstance.getTransaction().rollback();
                }
            }
        }

        public boolean isAlmostFinished() {
            return index > this.size / 2;
        }
    }

    public static class TakeThread extends Thread {
        private HazelcastInstance hazelcastInstance;
        private List<String> destination;
        private IMap map;
        private static int removeItemIndex = 0;

        public void setStop(boolean stop) {
            this.stop = stop;
        }

        private boolean stop;

        public TakeThread(IMap map, List<String> destination, HazelcastInstance hazelcastInstance) {
            this.map = map;
            this.destination = destination;
            this.hazelcastInstance = hazelcastInstance;
        }

        @Override
        public void run() {
            while (!stop) {
                hazelcastInstance.getTransaction().begin();
                String key = null;
                try {
                    System.out.println("Removing key");
                    Set<String> set = map.localKeySet();
                    ArrayList<String> strings = new ArrayList<String>(set);
                    if (!strings.isEmpty()) {
                        key = strings.get(new Random().nextInt(strings.size()));
                        String value = (String) map.tryRemove(key, 100, TimeUnit.MILLISECONDS);
//                        String value = (String) map.remove(key);
                        if (value != null) {
                            destination.add(value);
                            removeItemIndex++;
                            System.out.println("Removing key " + key + " successfully");
                        }
                    }
                    hazelcastInstance.getTransaction().commit();
                    System.out.println("Commiting on take " + key);
                } catch (Exception e) {
                    e.printStackTrace();
                    hazelcastInstance.getTransaction().rollback();
                    System.out.println(Thread.currentThread().getName() + ": Exception wile consuming object: " + e.getMessage() + " key: " + key);
                }
            }
        }
    }

    private static HazelcastInstance createInstance(String clusterId, int clusterNodeId) {
        Config config = new Config();
        MapConfig defaultMapConfig = new MapConfig();
        defaultMapConfig.setName("default");
        //There's a tinny chance that I end up reading old information from backup
//        defaultMapConfig.setReadBackupData(true);
        config.addMapConfig(defaultMapConfig);
        config.getManagementCenterConfig().setEnabled(true).setUrl("http://localhost:8080/mancenter");
        return Hazelcast.newHazelcastInstance(config);
    }
}
