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

package com.hazelcast.core.standalone;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.query.impl.predicate.SqlPredicate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * A test of queues, topics, Maps, AtomicInteger etc.
 */
public class AllTest {

    private static final int ONE_SECOND = 1000;
    private static final int STATS_SECONDS = 10;
    private static final int SIZE = 10000;
    final Logger logger = Logger.getLogger("All-test");
    final HazelcastInstance hazelcast;
    private volatile boolean running = true;
    private final int nThreads;
    private final List<Runnable> operations = new ArrayList<Runnable>();
    private final ExecutorService ex;
    private final Random random = new Random();
    private final AtomicInteger messagesReceived = new AtomicInteger(0);
    private final AtomicInteger messagesSend = new AtomicInteger(0);


    AllTest(int nThreads) {
        this.nThreads = nThreads;
        ex = Executors.newFixedThreadPool(nThreads);
        hazelcast = Hazelcast.newHazelcastInstance(null);
        List<Runnable> mapOperations = loadMapOperations();
        List<Runnable> qOperations = loadQOperations();
        List<Runnable> topicOperations = loadTopicOperations();
        this.operations.addAll(mapOperations);
        this.operations.addAll(qOperations);
        this.operations.addAll(topicOperations);
        Collections.shuffle(operations);
    }

    /**
     * Starts the test
     * @param args the number of threads to start
     */
    public static void main(String[] args) {
        int nThreads = (args.length == 0) ? 10 : Integer.parseInt(args[0]);
        final AllTest allTest = new AllTest(nThreads);
        allTest.start();
        Executors.newSingleThreadExecutor().execute(new Runnable() {

            public void run() {
                while (true) {
                    try {
                        //noinspection BusyWait
                        Thread.sleep(STATS_SECONDS * ONE_SECOND);
                        System.out.println("cluster SIZE:"
                                + allTest.hazelcast.getCluster().getMembers().size());
                        allTest.mapStats();
                        allTest.qStats();
                        allTest.topicStats();
                    } catch (InterruptedException ignored) {
                        return;
                    }
                }
            }
        });
    }

    private void qStats() {
//        LocalQueueOperationStats qOpStats = hazelcast.getQueue("myQ").getLocalQueueStats().getOperationStats();
//        long period = ((qOpStats.getPeriodEnd() - qOpStats.getPeriodStart()) / 1000);
//        if (period == 0) {
//            return;
//        }
//        log(qOpStats);
//        log("Q Operations per Second : " + (qOpStats.getOfferOperationCount() + qOpStats.getEmptyPollOperationCount() +
// qOpStats.getEmptyPollOperationCount() + qOpStats.getRejectedOfferOperationCount()) / period);
    }

    private void log(Object message) {
        if (message != null) {
            logger.info(message.toString());
        }
    }

    private void mapStats() {
//        LocalMapOperationStats mapOpStats = hazelcast.getMap("myMap").getLocalMapStats().getOperationStats();
//        long period = ((mapOpStats.getPeriodEnd() - mapOpStats.getPeriodStart()) / 1000);
//        if (period == 0) {
//            return;
//        }
//        log(mapOpStats);
//        log("Map Operations per Second : " + mapOpStats.total() / period);
    }

    private void topicStats() {
        log("Topic Messages Sent : " + messagesSend.getAndSet(0) / STATS_SECONDS + "::: Messages Received: " + messagesReceived
                .getAndSet(0) / STATS_SECONDS);
    }


    private void addOperation(List<Runnable> operations, Runnable runnable, int priority) {
        for (int i = 0; i < priority; i++) {
            operations.add(runnable);
        }
    }

    private void start() {
        for (int i = 0; i < nThreads; i++) {
            ex.submit(new Runnable() {
                public void run() {
                    while (running) {
                        int opId = random.nextInt(operations.size());
                        Runnable operation = operations.get(opId);
                        operation.run();
//                        System.out.println("Runnning..." + Thread.currentThread());
                    }
                }
            });
        }
    }

    private void stop() {
        running = false;
    }

    /**
     * An example customer class
     */
    public static class Customer implements Serializable {
        private int year;
        private String name;
        private byte[] field = new byte[100];

        public Customer(int i, String s) {
            this.year = i;
            this.name = s;
        }
    }

    private List<Runnable> loadTopicOperations() {
        ITopic topic = hazelcast.getTopic("myTopic");
        topic.addMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                messagesReceived.incrementAndGet();
            }
        });
        List<Runnable> operations = new ArrayList<Runnable>();
        addOperation(operations, new Runnable() {
            public void run() {
                ITopic topic = hazelcast.getTopic("myTopic");
                topic.publish(String.valueOf(random.nextInt(100000000)));
                messagesSend.incrementAndGet();
            }
        }, 10);
        return operations;
    }

    private List<Runnable> loadQOperations() {
        List<Runnable> operations = new ArrayList<Runnable>();
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = hazelcast.getQueue("myQ");
                q.offer(new byte[100]);
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = hazelcast.getQueue("myQ");
                try {
                    q.offer(new byte[100], 10, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = hazelcast.getQueue("myQ");
                q.contains(new byte[100]);
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = hazelcast.getQueue("myQ");
                q.isEmpty();
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = hazelcast.getQueue("myQ");
                q.size();
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = hazelcast.getQueue("myQ");
                q.remove(new byte[100]);
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = hazelcast.getQueue("myQ");
                q.remainingCapacity();
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = hazelcast.getQueue("myQ");
                q.poll();
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = hazelcast.getQueue("myQ");
                q.add(new byte[100]);
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = hazelcast.getQueue("myQ");
                try {
                    q.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = hazelcast.getQueue("myQ");
                List list = new ArrayList();
                for (int i = 0; i < 10; i++) {
                    list.add(new byte[100]);
                }
                q.addAll(list);
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = hazelcast.getQueue("myQ");
                List list = new ArrayList();
                q.drainTo(list);
            }
        }, 1);
        return operations;
    }

    private List<Runnable> loadMapOperations() {
        ArrayList<Runnable> operations = new ArrayList<Runnable>();
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.evict(random.nextInt(SIZE));
            }
        }, 5);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                try {
                    map.getAsync(random.nextInt(SIZE)).get();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.containsKey(random.nextInt(SIZE));
            }
        }, 2);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.containsValue(new Customer(random.nextInt(100), String.valueOf(random.nextInt(100000))));
            }
        }, 2);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                int key = random.nextInt(SIZE);
                map.lock(key);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    map.unlock(key);
                }
            }
        }, 1);
//        addOperation(operations, new Runnable() {
//            public void run() {
//                IMap map = hazelcast.getMap("myMap");
//                int key = random.nextInt(SIZE);
//                map.lockMap(10, TimeUnit.MILLISECONDS);
//                try {
//                    Thread.sleep(1);
//                } catch (InterruptedException e) {
//                } finally {
//                    map.unlockMap();
//                }
//            }
//        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                int key = random.nextInt(SIZE);
                boolean locked = map.tryLock(key);
                if (locked) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } finally {
                        map.unlock(key);
                    }
                }
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                int key = random.nextInt(SIZE);
                boolean locked = false;
                try {
                    locked = map.tryLock(key, 10, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (locked) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } finally {
                        map.unlock(key);
                    }
                }
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                Iterator it = map.entrySet().iterator();
                for (int i = 0; i < 10 && it.hasNext(); i++) {
                    it.next();
                }
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.getEntryView(random.nextInt(SIZE));
            }
        }, 2);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.isEmpty();
            }
        }, 3);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.put(random.nextInt(SIZE), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))));
            }
        }, 50);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.tryPut(random.nextInt(SIZE), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))), 10,
                        TimeUnit.MILLISECONDS);
            }
        }, 5);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                try {
                    map.putAsync(random.nextInt(SIZE), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000)))
                    ).get();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 5);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.put(random.nextInt(SIZE), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))), 10,
                        TimeUnit.MILLISECONDS);
            }
        }, 5);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.putIfAbsent(random.nextInt(SIZE), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))),
                        10, TimeUnit.MILLISECONDS);
            }
        }, 5);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.putIfAbsent(random.nextInt(SIZE), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))));
            }
        }, 5);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                Map localMap = new HashMap();
                for (int i = 0; i < 10; i++) {
                    localMap.put(random.nextInt(SIZE), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))));
                }
                map.putAll(localMap);
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.get(random.nextInt(SIZE));
            }
        }, 100);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.remove(random.nextInt(SIZE));
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.tryRemove(random.nextInt(SIZE), 10, TimeUnit.MILLISECONDS);
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.removeAsync(random.nextInt(SIZE));
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.remove(random.nextInt(SIZE), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))));
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.replace(random.nextInt(SIZE), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))));
            }
        }, 4);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.replace(random.nextInt(SIZE), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))),
                        new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))));
            }
        }, 5);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.size();
            }
        }, 4);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                Iterator it = map.entrySet(SqlPredicate.createPredicate("year=" + random.nextInt(100))).iterator();
                while (it.hasNext()) {
                    it.next();
                }
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                Iterator it = map.entrySet(SqlPredicate.createPredicate("name=" + random.nextInt(10000))).iterator();
                while (it.hasNext()) {
                    it.next();
                }
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                Iterator it = map.keySet(SqlPredicate.createPredicate("name=" + random.nextInt(10000))).iterator();
                while (it.hasNext()) {
                    it.next();
                }
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                Iterator it = map.localKeySet().iterator();
                while (it.hasNext()) {
                    it.next();
                }
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                Iterator it = map.localKeySet(SqlPredicate.createPredicate("name=" + random.nextInt(10000))).iterator();
                while (it.hasNext()) {
                    it.next();
                }
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                final CountDownLatch latch = new CountDownLatch(1);
                EntryListener listener = new EntryAdapter() {
                    @Override
                    public void onEntryEvent(EntryEvent event) {
                        latch.countDown();
                    }
                };
                String id = map.addEntryListener(listener, true);
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                map.removeEntryListener(id);
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                map.addIndex("year", true);
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = hazelcast.getMap("myMap");
                final CountDownLatch latch = new CountDownLatch(1);
                EntryListener listener = new EntryAdapter() {
                    @Override
                    public void onEntryEvent(EntryEvent event) {
                        latch.countDown();
                    }
                };
                String id = map.addLocalEntryListener(listener);
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                map.removeEntryListener(id);
            }
        }, 1);
        return operations;
    }
}
