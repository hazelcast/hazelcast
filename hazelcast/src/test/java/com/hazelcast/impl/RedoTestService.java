/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.core.*;
import org.junit.Ignore;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.fail;

@Ignore
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class RedoTestService extends TestUtil {

    @Ignore
    abstract class BeforeAfterBehavior {
        abstract void before() throws Exception;

        abstract void after();

        protected void destroy() {
        }
    }

    @Ignore
    abstract class CallBuilder {
        abstract List<FutureTask> getCalls(ExecutorService es, ExecutorService esSingle);

        protected void addRunnable(ExecutorService es, List<FutureTask> lsFutures, Callable callable) {
            FutureTask f = new PrintableFutureTask(callable);
            lsFutures.add(f);
            es.execute(f);
        }

        class PrintableFutureTask extends FutureTask {
            Object callOrRunnable;

            PrintableFutureTask(Callable callable) {
                super(callable);
                this.callOrRunnable = callable;
            }

            PrintableFutureTask(Runnable runnable, Object result) {
                super(runnable, result);
                this.callOrRunnable = runnable;
            }

            @Override
            public String toString() {
                return "Task [" + callOrRunnable + "]";
            }
        }
    }

    @Ignore
    class BeforeAfterTester implements Runnable {
        final protected ExecutorService es = Executors.newCachedThreadPool();
        final protected ExecutorService esSingle = Executors.newSingleThreadExecutor();

        final BeforeAfterBehavior behavior;
        final CallBuilder callBuilder;

        BeforeAfterTester(BeforeAfterBehavior behavior, CallBuilder callBuilder) {
            this.behavior = behavior;
            this.callBuilder = callBuilder;
        }

        protected void destroy() {
            es.shutdown();
            esSingle.shutdown();
        }

        public void run() {
            try {
                try {
                    behavior.before();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }
                List<FutureTask> lsFutureTasks = callBuilder.getCalls(es, esSingle);
                for (FutureTask futureTask : lsFutureTasks) {
                    try {
                        Object result = futureTask.get(0, TimeUnit.SECONDS);
                        fail("Expected: TimeoutException, got " + result + ", callTask: " + futureTask);
                    } catch (TimeoutException e) {
                    } catch (Exception e) {
                        fail();
                        return;
                    }
                }
                behavior.after();
                for (FutureTask futureTask : lsFutureTasks) {
                    try {
                        futureTask.get(20, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("Failed callTask: " + futureTask);
                        return;
                    }
                }
            } finally {
                behavior.destroy();
                destroy();
            }
        }
    }

    @Ignore
    class RunAfterTester implements Runnable {
        final protected ExecutorService es = Executors.newCachedThreadPool();
        final protected ExecutorService esSingle = Executors.newSingleThreadExecutor();

        final BeforeAfterBehavior behavior;
        final CallBuilder callBuilder;

        RunAfterTester(BeforeAfterBehavior behavior, CallBuilder callBuilder) {
            this.behavior = behavior;
            this.callBuilder = callBuilder;
        }

        protected void destroy() {
            es.shutdown();
            esSingle.shutdown();
        }

        public void run() {
            try {
                try {
                    behavior.before();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }
                behavior.after();
                List<FutureTask> lsFutureTasks = callBuilder.getCalls(es, esSingle);
                for (FutureTask futureTask : lsFutureTasks) {
                    try {
                        futureTask.get(20, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("Failed callTask: " + futureTask);
                        return;
                    }
                }
            } finally {
                behavior.destroy();
                destroy();
            }
        }
    }

    @Ignore
    class QueueCallBuilder extends CallBuilder {
        final HazelcastInstance callerInstance;
        final IQueue q;

        protected QueueCallBuilder(HazelcastInstance callerInstance) {
            this.callerInstance = callerInstance;
            this.q = callerInstance.getQueue("default");
        }

        @Override
        List<FutureTask> getCalls(ExecutorService es, ExecutorService esSingle) {
            List<FutureTask> lsFutureTasks = new LinkedList<FutureTask>();
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return q.offer(1);
                }

                @Override
                public String toString() {
                    return "q.offer";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return q.offer(1);
                }

                @Override
                public String toString() {
                    return "q.offer";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return q.offer(1);
                }

                @Override
                public String toString() {
                    return "q.offer";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return q.poll();
                }

                @Override
                public String toString() {
                    return "q.poll";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return q.poll(20, TimeUnit.SECONDS);
                }

                @Override
                public String toString() {
                    return "q.poll with timeout";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return q.take();
                }

                @Override
                public String toString() {
                    return "q.take";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return q.peek();
                }

                @Override
                public String toString() {
                    return "q.peek";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return q.size();
                }

                @Override
                public String toString() {
                    return "q.size";
                }
            });
            return lsFutureTasks;
        }
    }

    @Ignore
    class KeyCallBuilder extends CallBuilder {

        final HazelcastInstance callerInstance;
        final IMap imap;

        protected KeyCallBuilder(HazelcastInstance callerInstance) {
            this.callerInstance = callerInstance;
            this.imap = callerInstance.getMap("default");
        }

        @Override
        List<FutureTask> getCalls(ExecutorService es, ExecutorService esSingle) {
            List<FutureTask> lsFutureTasks = new LinkedList<FutureTask>();
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return imap.get(1);
                }

                @Override
                public String toString() {
                    return "m.get";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return imap.put(1, "value1");
                }

                @Override
                public String toString() {
                    return "m.put";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return imap.evict(1);
                }

                @Override
                public String toString() {
                    return "m.evict";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return imap.remove(1);
                }

                @Override
                public String toString() {
                    return "m.remove";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return imap.getMapEntry(1);
                }

                @Override
                public String toString() {
                    return "m.getMapEntry";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return imap.containsKey(1);
                }

                @Override
                public String toString() {
                    return "m.containsKey";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return imap.getAsync(1).get();
                }

                @Override
                public String toString() {
                    return "m.getAsync";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return imap.putIfAbsent(1, "valuePutIfAbsent");
                }

                @Override
                public String toString() {
                    return "m.putIfAbsent";
                }
            });
            addRunnable(esSingle, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    imap.lock(1);
                    return Boolean.TRUE;
                }

                @Override
                public String toString() {
                    return "m.lock";
                }
            });
            addRunnable(esSingle, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    imap.unlock(1);
                    return Boolean.TRUE;
                }

                @Override
                public String toString() {
                    return "m.unlock";
                }
            });
            return lsFutureTasks;
        }
    }

    @Ignore
    class MultiCallBuilder extends CallBuilder {

        final HazelcastInstance callerInstance;
        final IMap imap;
        final IQueue q;
        final ITopic topic;

        protected MultiCallBuilder(HazelcastInstance callerInstance) {
            this.callerInstance = callerInstance;
            this.imap = callerInstance.getMap("default");
            this.q = callerInstance.getQueue("default");
            this.topic = callerInstance.getTopic("default");
        }

        @Override
        List<FutureTask> getCalls(ExecutorService es, ExecutorService esSingle) {
            List<FutureTask> lsFutureTasks = new LinkedList<FutureTask>();
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return imap.size();
                }

                @Override
                public String toString() {
                    return "m.size";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return imap.values();
                }

                @Override
                public String toString() {
                    return "m.values";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return imap.containsValue(1);
                }

                @Override
                public String toString() {
                    return "m.containsValue";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    imap.addEntryListener(new EntryAdapter(), true);
                    return Boolean.TRUE;
                }

                @Override
                public String toString() {
                    return "m.addEntryListener";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return imap.keySet();
                }

                @Override
                public String toString() {
                    return "m.keySet";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    return imap.entrySet();
                }

                @Override
                public String toString() {
                    return "m.entrySet";
                }
            });
            addRunnable(es, lsFutureTasks, new Callable() {
                public Object call() throws Exception {
                    topic.addMessageListener(new MessageListener() {
                        public void onMessage(Message message) {
                        }
                    });
                    return Boolean.TRUE;
                }

                @Override
                public String toString() {
                    return "t.addMessageListener";
                }
            });
//            TODO
//            addRunnable(es, lsFutureTasks, new Callable() {
//                public Object call() throws Exception {
//                    topic.publish(1);
//                    return Boolean.TRUE;
//                }
//
//                @Override
//                public String toString() {
//                    return "t.publish";
//                }
//            });
            return lsFutureTasks;
        }
    }
}
