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

package com.hazelcast.examples;

import com.hazelcast.core.*;
import com.hazelcast.util.Clock;
import com.hazelcast.monitor.LocalMapOperationStats;
import com.hazelcast.monitor.LocalQueueOperationStats;
import com.hazelcast.query.SqlPredicate;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class AllTest {

    private volatile boolean running = true;
    private final int nThreads;
    private final List<Runnable> operations = new ArrayList<Runnable>();
    private final ExecutorService ex;
    private final Random random = new Random(System.nanoTime());
    private final AtomicInteger messagesReceived = new AtomicInteger(0);
    private final AtomicInteger messagesSend = new AtomicInteger(0);

    private final int size = 10000;
    private static final int STATS_SECONDS = 10;

    final Logger logger = Logger.getLogger("All-test");

    public static void main(String[] args) {
        int nThreads = (args.length == 0) ? 10 : new Integer(args[0]);
        final AllTest allTest = new AllTest(nThreads);
        allTest.start();
        Executors.newSingleThreadExecutor().execute(new Runnable() {

            public void run() {
                while (true) {
                    try {
                        //noinspection BusyWait
                        Thread.sleep(STATS_SECONDS * 1000);
                        System.out.println("cluster size:"
                                + Hazelcast.getCluster().getMembers().size());
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
        LocalQueueOperationStats qOpStats = Hazelcast.getQueue("myQ").getLocalQueueStats().getOperationStats();
        long period = ((qOpStats.getPeriodEnd() - qOpStats.getPeriodStart()) / 1000);
        if (period == 0) {
            return;
        }
        log(qOpStats);
        log("Q Operations per Second : " + (qOpStats.getNumberOfOffers() + qOpStats.getNumberOfEmptyPolls() + qOpStats.getNumberOfEmptyPolls() + qOpStats.getNumberOfRejectedOffers()) / period);
    }

    private void log(Object message) {
        if (message != null) {
            logger.info(message.toString());
        }
    }

    private void mapStats() {
        LocalMapOperationStats mapOpStats = Hazelcast.getMap("myMap").getLocalMapStats().getOperationStats();
        long period = ((mapOpStats.getPeriodEnd() - mapOpStats.getPeriodStart()) / 1000);
        if (period == 0) {
            return;
        }
        log(mapOpStats);
        log("Map Operations per Second : " + mapOpStats.total() / period);
    }

    private void topicStats() {
        log("Topic Messages Sent : " + messagesSend.getAndSet(0) / STATS_SECONDS + "::: Messages Received: " + messagesReceived.getAndSet(0) / STATS_SECONDS);
    }

    AllTest(int nThreads) {
        this.nThreads = nThreads;
        ex = Executors.newFixedThreadPool(nThreads);
        List<Runnable> mapOperations = loadMapOperations();
        List<Runnable> qOperations = loadQOperations();
        List<Runnable> topicOperations = loadTopicOperations();
        this.operations.addAll(mapOperations);
        this.operations.addAll(qOperations);
        this.operations.addAll(topicOperations);
        Collections.shuffle(operations);
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
        ITopic topic = Hazelcast.getTopic("myTopic");
        topic.addMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                messagesReceived.incrementAndGet();
            }
        });
        List<Runnable> operations = new ArrayList<Runnable>();
        addOperation(operations, new Runnable() {
            public void run() {
                ITopic topic = Hazelcast.getTopic("myTopic");
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
                IQueue q = Hazelcast.getQueue("myQ");
                q.offer(new byte[100]);
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = Hazelcast.getQueue("myQ");
                try {
                    q.offer(new byte[100], 10, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = Hazelcast.getQueue("myQ");
                q.contains(new byte[100]);
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = Hazelcast.getQueue("myQ");
                q.isEmpty();
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = Hazelcast.getQueue("myQ");
                q.size();
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = Hazelcast.getQueue("myQ");
                q.remove(new byte[100]);
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = Hazelcast.getQueue("myQ");
                q.remainingCapacity();
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = Hazelcast.getQueue("myQ");
                q.poll();
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = Hazelcast.getQueue("myQ");
                q.add(new byte[100]);
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = Hazelcast.getQueue("myQ");
                try {
                    q.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = Hazelcast.getQueue("myQ");
                List list = new ArrayList();
                for (int i = 0; i < 10; i++) {
                    list.add(new byte[100]);
                }
                q.addAll(list);
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IQueue q = Hazelcast.getQueue("myQ");
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
                IMap map = Hazelcast.getMap("myMap");
                map.evict(random.nextInt(size));
            }
        }, 5);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                try {
                    map.getAsync(random.nextInt(size)).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                } catch (ExecutionException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                map.containsKey(random.nextInt(size));
            }
        }, 2);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                map.containsValue(new Customer(random.nextInt(100), String.valueOf(random.nextInt(100000))));
            }
        }, 2);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                int key = random.nextInt(size);
                map.lock(key);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                } finally {
                    map.unlock(key);
                }
            }
        }, 1);
//        addOperation(operations, new Runnable() {
//            public void run() {
//                IMap map = Hazelcast.getMap("myMap");
//                int key = random.nextInt(size);
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
                IMap map = Hazelcast.getMap("myMap");
                int key = random.nextInt(size);
                boolean locked = map.tryLock(key);
                if (locked) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                    } finally {
                        map.unlock(key);
                    }
                }
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                int key = random.nextInt(size);
                boolean locked = map.tryLock(key, 10, TimeUnit.MILLISECONDS);
                if (locked) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                    } finally {
                        map.unlock(key);
                    }
                }
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                Iterator it = map.entrySet().iterator();
                for (int i = 0; i < 10 && it.hasNext(); i++) {
                    it.next();
                }
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                map.getMapEntry(random.nextInt(size));
            }
        }, 2);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                map.isEmpty();
            }
        }, 3);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                map.put(random.nextInt(size), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))));
            }
        }, 50);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                map.tryPut(random.nextInt(size), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))), 10, TimeUnit.MILLISECONDS);
            }
        }, 5);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                try {
                    map.putAsync(random.nextInt(size), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000)))).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                } catch (ExecutionException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }, 5);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                map.put(random.nextInt(size), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))), 10, TimeUnit.MILLISECONDS);
            }
        }, 5);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                map.putIfAbsent(random.nextInt(size), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))), 10, TimeUnit.MILLISECONDS);
            }
        }, 5);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                map.putIfAbsent(random.nextInt(size), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))));
            }
        }, 5);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                Map localMap = new HashMap();
                for (int i = 0; i < 10; i++) {
                    localMap.put(random.nextInt(size), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))));
                }
                map.putAll(localMap);
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                map.get(random.nextInt(size));
            }
        }, 100);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                map.remove(random.nextInt(size));
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                try {
                    map.tryRemove(random.nextInt(size), 10, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                map.removeAsync(random.nextInt(size));
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                map.remove(random.nextInt(size), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))));
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                map.replace(random.nextInt(size), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))));
            }
        }, 4);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                map.replace(random.nextInt(size), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))), new Customer(random.nextInt(100), String.valueOf(random.nextInt(10000))));
            }
        }, 5);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                map.size();
            }
        }, 4);
        addOperation(operations, new Runnable() {
            public void run() {
                long begin = Clock.currentTimeMillis();
                IMap map = Hazelcast.getMap("myMap");
                Iterator it = map.entrySet(new SqlPredicate("year=" + random.nextInt(100))).iterator();
                while (it.hasNext()) {
                    it.next();
                }
//                System.out.println("Took: " + (Clock.currentTimeMillis() - begin));
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                Iterator it = map.entrySet(new SqlPredicate("name=" + random.nextInt(10000))).iterator();
                while (it.hasNext()) {
                    it.next();
                }
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                Iterator it = map.keySet(new SqlPredicate("name=" + random.nextInt(10000))).iterator();
                while (it.hasNext()) {
                    it.next();
                }
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                Iterator it = map.localKeySet().iterator();
                while (it.hasNext()) {
                    it.next();
                }
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                Iterator it = map.localKeySet(new SqlPredicate("name=" + random.nextInt(10000))).iterator();
                while (it.hasNext()) {
                    it.next();
                }
            }
        }, 10);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                final CountDownLatch latch = new CountDownLatch(1);
                EntryListener listener = new EntryListener() {
                    public void entryAdded(EntryEvent entryEvent) {
                        latch.countDown();
                    }

                    public void entryRemoved(EntryEvent entryEvent) {
                        latch.countDown();
                    }

                    public void entryUpdated(EntryEvent entryEvent) {
                        latch.countDown();
                    }

                    public void entryEvicted(EntryEvent entryEvent) {
                        latch.countDown();
                    }
                };
                map.addEntryListener(listener, true);
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
                map.removeEntryListener(listener);
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                map.addIndex("year", true);
            }
        }, 1);
        addOperation(operations, new Runnable() {
            public void run() {
                IMap map = Hazelcast.getMap("myMap");
                final CountDownLatch latch = new CountDownLatch(1);
                EntryListener listener = new EntryListener() {
                    public void entryAdded(EntryEvent entryEvent) {
                        latch.countDown();
                    }

                    public void entryRemoved(EntryEvent entryEvent) {
                        latch.countDown();
                    }

                    public void entryUpdated(EntryEvent entryEvent) {
                        latch.countDown();
                    }

                    public void entryEvicted(EntryEvent entryEvent) {
                        latch.countDown();
                    }
                };
                map.addLocalEntryListener(listener);
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
                map.removeEntryListener(listener);
            }
        }, 1);
        return operations;
    }
}
