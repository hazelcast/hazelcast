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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.*;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.test.*;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ReplicatedMapPlay extends HazelcastTestSupport {


    @Test(expected = java.lang.IllegalArgumentException.class)
    public void putNullKey() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        Config cfg = new Config();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");

        map1.put(null, 1);
    }


    @Test(expected = java.lang.IllegalArgumentException.class)
    public void removeNullKey() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        Config cfg = new Config();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");

        map1.remove(null);
    }


    @Test
    public void removeEmptyListener() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        Config cfg = new Config();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");

        assertFalse(map1.removeEntryListener("2"));
    }


    @Test(expected = java.lang.IllegalArgumentException.class)
    public void removeNullListener() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        Config cfg = new Config();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");

        map1.removeEntryListener(null);
    }



    @Test
    public void equalsTest() throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);

        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(0);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> map2 = instance2.getReplicatedMap("default");

        for(int i=0; i<1000; i++){
            map1.put(i, i);
        }

        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(1000, map1.size());
                assertEquals(1000, map2.size());

                assertTrue(map1.equals(map2));
                assertTrue(map1.hashCode() == map2.hashCode());

                assertTrue(map1.entrySet().equals(map2.entrySet()));
                assertTrue(map1.values().equals(map2.values()));
            }
        });
    }



    @Test
    public void sameMap_putTTLandPut_allMostSimil_repDelay0() throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);

        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(0);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> map2 = instance2.getReplicatedMap("default");


        map1.put(1, 1, 1, TimeUnit.SECONDS);
        map1.put(1, 1);

        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(1, map1.get(1));
                assertEquals(1, map2.get(1));
            }
        });

        map1.put(1, 1, 1, TimeUnit.SECONDS);

        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(null, map1.get(1));
                assertEquals(null, map2.get(1));
            }
        });
    }


    @Ignore("The update Conflict value is determined by the lastUpdateHash from the recored Store")
    @Test()
    public void putOrderTest_repDelay0__CONST_FAIL() throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(0);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> map2 = instance2.getReplicatedMap("default");


        map1.put(1, 1);
        map2.put(1, 2);

        Thread.sleep(500);

        Set<Entry<Object, Object>> s = map1.entrySet();
        System.out.println(s.size());
        Object[] x = s.toArray();
        x[0].hashCode();

        s = map2.entrySet();
        System.out.println(s.size());
        Object[] y = s.toArray();
        y[0].hashCode();


        System.out.println( x[0].hashCode() + "   " + y[0].hashCode() );

/*
        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(1, map1.get(1));
            }
        });
*/

        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(2, map2.get(1));
            }
        });

        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(2, map1.get(1));
            }
        });
    }


    @Ignore("The update Conflict value is determined by the lastUpdateHash from the recored Store")
    @Test
    public void putOrderTest_repDelay1000_FAILS_RANDOM() throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(1000);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> map2 = instance2.getReplicatedMap("default");

        Integer a = new Integer(1);
        Integer b = new Integer(1);

        System.out.println("obj="+a+" hash=>"+a.hashCode());
        System.out.println("obj=" + b + " hash=>" + b.hashCode());

        map1.put(1, 1);
        map2.put(1, 2);

        //map1.put(a, 1);
        //map2.put(a, 2);

        //map1.put(a, 1);
        //map2.put(b, 2);


        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(2, map1.get(1));
            }
        });

        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(2, map2.get(1));
            }
        });
    }




    @Test
    public void putTTL_Vs_put_repDelay0() throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);

        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(0);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> map2 = instance2.getReplicatedMap("default");


        map1.put(1, 1, 1, TimeUnit.SECONDS);
        //if we put a sneek sleep Test passes but why we need a sleep the order is the order ?
        map2.put(1, 1);


        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(1, map1.get(1));
            }
        });

        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(1, map2.get(1));
            }
        });


        map1.put(1, 1);
        map2.put(1, 1, 1, TimeUnit.SECONDS);


        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(null, map1.get(1));
            }
        });

        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(null, map2.get(1));
            }
        });
    }


    @Ignore("The update Conflict value is determined by the lastUpdateHash from the recored Store")
    @Test
    public void putTTL_Vs_put_repDelay1000_FAILS_RANDOM() throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);

        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(1000);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> map2 = instance2.getReplicatedMap("default");


        map1.put(1, 1, 1, TimeUnit.SECONDS);
        map2.put(1, 1);

        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(1, map1.get(1));
            }
        });

        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(1, map2.get(1));
            }
        });

        map1.put(2, 2);
        map2.put(2, 2, 1, TimeUnit.SECONDS);

        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(null, map1.get(2));
            }
        });

        //This test random fails
        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(null, map2.get(2));
            }
        });

    }


    @Test
    public void repMap_InMemoryFormatConflict() throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg1 = new Config();
        cfg1.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        cfg1.getReplicatedMapConfig("default").setReplicationDelayMillis(0);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg1);

        Config cfg2 = new Config();
        cfg2.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        cfg2.getReplicatedMapConfig("default").setReplicationDelayMillis(0);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg2);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> map2 = instance2.getReplicatedMap("default");

        map1.put(1, 1);
        map2.put(2, 2);


        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(1, map1.get(1));
                assertEquals(1, map2.get(1));

                assertEquals(2, map1.get(2));
                assertEquals(2, map2.get(2));
            }
        });
    }



    @Test
    public void repMap_RepDelayConflict() throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg1 = new Config();
        cfg1.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        cfg1.getReplicatedMapConfig("default").setReplicationDelayMillis(0);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg1);

        Config cfg2 = new Config();
        cfg2.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        cfg2.getReplicatedMapConfig("default").setReplicationDelayMillis(1000);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg2);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> map2 = instance2.getReplicatedMap("default");

        map1.put(1, 1);
        map2.put(2, 2);

        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(null, map1.get(2));
                assertEquals(1,    map2.get(1));
            }
        });

        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(2, map1.get(2));
            }
        });

    }


    @Test
    public void MultiReplicationDataTest_repDelay0() throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(0);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);


        final ReplicatedMap<Object, Object> mapA = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapB = instance2.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapC = instance3.getReplicatedMap("default");

        for(int i=0; i<1000; i++){
            mapA.put(i, i);
        }

        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                for(int i=0; i<1000; i++){
                    assertEquals(i, mapA.get(i));
                    assertEquals(i, mapB.get(i));
                    assertEquals(i, mapC.get(i));
                }
            }
        });
    }

    @Test
    public void MapRep3wayTest_Delay0() throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(0);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);


        final ReplicatedMap<Object, Object> mapA = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapB = instance2.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapC = instance3.getReplicatedMap("default");

        for(int i=0; i<1000; i++){
            mapA.put(i+"A", i+"A");
            mapB.put(i+"B", i+"B");
        }

        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                for(int i=0; i<1000; i++){
                    assertEquals(i+"A", mapA.get(i+"A"));
                    assertEquals(i+"A", mapB.get(i+"A"));
                    assertEquals(i+"A", mapC.get(i+"A"));
                    assertEquals(i+"B", mapA.get(i+"B"));
                    assertEquals(i+"B", mapB.get(i+"B"));
                    assertEquals(i+"B", mapC.get(i+"B"));
                }
            }
        });
    }

    @Test
    public void multiPutThreads_dealy0() throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(0);
        cfg.getReplicatedMapConfig("default").setConcurrencyLevel(1);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> mapA = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapB = instance2.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapC = instance3.getReplicatedMap("default");


        Thread[] pool = new Thread[10];
        CyclicBarrier gate = new CyclicBarrier(pool.length+1);
        for(int i=0; i<5; i++){
            pool[i] = new GatedThread(gate) {
                public void go() {
                    for(int i=0; i<1000; i++)
                        mapA.put(i+"A", i);
                }
            };
            pool[i].start();
        }
        for(int i=5; i<10; i++){
            pool[i] = new GatedThread(gate) {
                public void go() {
                    for(int i=0; i<1000; i++)
                        mapB.put(i+"B", i);
                }
            };
            pool[i].start();
        }
        gate.await();

        for(Thread t:pool){
            t.join();
        }


        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                for(int i=0; i<1000; i++){
                    assertEquals(i, mapA.get(i+"A"));
                    assertEquals(i, mapB.get(i + "A"));
                    assertEquals(i, mapC.get(i+"A"));

                    assertEquals(i, mapA.get(i+"B"));
                    assertEquals(i, mapB.get(i+"B"));
                    assertEquals(i, mapC.get(i+"B"));
                }
            }
        });
    }


    @Test
    public void multiPutThreads_withNodeCrash() throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(0);
        cfg.getReplicatedMapConfig("default").setConcurrencyLevel(1);

        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> mapA = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapB = instance2.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapC = instance3.getReplicatedMap("default");

        Thread[] pool = new Thread[10];
        CyclicBarrier gate = new CyclicBarrier(pool.length+1);
        for(int i=0; i<1; i++){
            pool[i] = new GatedThread(gate) {
                public void go() {
                    for(int i=0; i<1000; i++){
                        if(i<500){
                            mapA.put(i+"A", i);
                        }
                        else if(i==500){
                            mapC.put(i+"C", i);
                            try {
                                Thread.sleep(500);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            TestUtil.terminateInstance(instance1);
                        }
                        else{
                            mapC.put(i+"C", i);
                        }
                    }
                }
            };
            pool[i].start();
        }
        for(int i=1; i<10; i++){
            pool[i] = new GatedThread(gate) {
                public void go() {
                    for(int i=0; i<1000; i++)
                        mapB.put(i+"B", i);
                }
            };
            pool[i].start();
        }



        gate.await();

        for(Thread t:pool){
            t.join();
        }


        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                for(int i=0; i<500; i++){
                    assertEquals(i, mapB.get(i+"A"));
                    assertEquals(i, mapC.get(i+"A"));
                    assertEquals(i, mapB.get(i+"B"));
                    assertEquals(i, mapC.get(i+"B"));
                }

                for(int i=500; i<1000; i++){
                    assertEquals(i, mapB.get(i+"C"));
                    assertEquals(i, mapC.get(i+"C"));
                    assertEquals(i, mapB.get(i+"B"));
                    assertEquals(i, mapC.get(i+"B"));
                }
            }
        });

    }



    @Test
    public void threadPuts_delay0() throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(0);
        cfg.getReplicatedMapConfig("default").setConcurrencyLevel(1);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> mapA = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapB = instance2.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapC = instance3.getReplicatedMap("default");


        Thread[] pool = new Thread[2];
        CyclicBarrier gate = new CyclicBarrier(pool.length+1);
        pool[0] = new GatedThread(gate) {
            public void go() {
                for(int i=0; i<1000; i++){
                    mapA.put(i+"A", i);
                }
            }
        };
        pool[1] = new GatedThread(gate) {
            public void go() {
                for(int i=0; i<1000; i++)
                    mapB.put(i+"B", i);
            }
        };

        for(Thread t:pool)
            t.start();

        gate.await();

        for(Thread t:pool)
            t.join();


        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                assertEquals(2000, mapA.size());
                assertEquals(2000, mapB.size());
                assertEquals(2000, mapC.size());
            }
        });



        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){
                for(int i=0; i<1000; i++){
                    assertEquals(i, mapA.get(i+"A"));
                    assertEquals(i, mapB.get(i + "A"));
                    assertEquals(i, mapC.get(i+"A"));

                    assertEquals(i, mapA.get(i+"B"));
                    assertEquals(i, mapB.get(i+"B"));
                    assertEquals(i, mapC.get(i+"B"));
                }
            }
        });
    }


    abstract public class GatedThread extends Thread{
        private final CyclicBarrier gate;

        public GatedThread(CyclicBarrier gate){
            this.gate = gate;
        }

        public void run(){
            try {
                gate.await();
                go();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        }

        abstract public void go();
    }


    @Ignore("The update Conflict value is determined by the lastUpdateHash from the recored Store")
    @Test
    public void MapRepInterleavedDataOrderTest_Delay0_CONST_FAILS() throws Exception {

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(0);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);


        final ReplicatedMap<Object, Object> mapA = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapB = instance2.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapC = instance3.getReplicatedMap("default");

        for(int i=0; i<1000; i++){
            if(i%2==0){
                mapB.put(i, i+"B");
                mapA.put(i, i+"A");
            }else{
                mapA.put(i, i+"A");
                mapB.put(i, i+"B");
            }
        }

        HazelcastTestSupport.assertTrueEventually(new AssertTask(){
            public void run(){

                for(int i=0; i<1000; i++){
                    if(i%2==0){
                        assertEquals(i+"A", mapA.get(i));
                        assertEquals(i+"A", mapB.get(i));
                        assertEquals(i+"A", mapC.get(i));
                    }else{
                        assertEquals(i+"B", mapA.get(i));
                        assertEquals(i+"B", mapB.get(i));
                        assertEquals(i+"B", mapC.get(i));
                    }
                }
            }
        });

    }


}
