/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitSupport;
import com.hazelcast.spi.annotation.PrivateApi;

import java.util.Iterator;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;

@PrivateApi
class WaitNotifyService {
    private final ConcurrentMap<Object, Queue<WaitingOp>> mapWaitingOps = new ConcurrentHashMap<Object, Queue<WaitingOp>>(100);
    private final DelayQueue delayQueue = new DelayQueue();
    private final ExecutorService esExpirationTaskExecutor = Executors.newSingleThreadExecutor();
    private final Future expirationTask;
    private final WaitingOpProcessor waitingOpProcessor;

    public WaitNotifyService(final WaitingOpProcessor waitingOpProcessor) {
        this.waitingOpProcessor = waitingOpProcessor;
        expirationTask = esExpirationTaskExecutor.submit(new Runnable() {
            public void run() {
                while (true) {
                    if (Thread.interrupted()) {
                        return;
                    }
                    try {
                        long waitTime = 1000;
                        while (waitTime > 0) {
                            long begin = System.currentTimeMillis();
                            WaitingOp waitingOp = (WaitingOp) delayQueue.poll(waitTime, TimeUnit.MILLISECONDS);
                            if (waitingOp != null) {
                                if (waitingOp.isValid()) {
                                    waitingOpProcessor.process(waitingOp);
                                }
                            }
                            long end = System.currentTimeMillis();
                            waitTime -= (end - begin);
                            if (waitTime > 1000) {
                                waitTime = 0;
                            }
                        }
                        for (Queue<WaitingOp> q : mapWaitingOps.values()) {
                            Iterator<WaitingOp> it = q.iterator();
                            while (it.hasNext()) {
                                if (Thread.interrupted()) {
                                    return;
                                }
                                WaitingOp waitingOp = it.next();
                                if (waitingOp.isValid() && waitingOp.expired()) {
                                    waitingOpProcessor.process(waitingOp);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        return;
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            }
        });
    }

    // runs after queue lock
    public void wait(WaitSupport so) {
        final Object key = so.getWaitKey();
        Queue<WaitingOp> q = mapWaitingOps.get(key);
        if (q == null) {
            q = new ConcurrentLinkedQueue<WaitingOp>();
            Queue<WaitingOp> qExisting = mapWaitingOps.putIfAbsent(key, q);
            if (qExisting != null) {
                q = qExisting;
            }
        }
        long timeout = so.getWaitTimeoutMillis();
        WaitingOp waitingOp = new WaitingOp(q, so);
        if (timeout > -1 && timeout < 1500) {
            delayQueue.offer(waitingOp);
        }
        q.offer(waitingOp);
    }

    // runs after queue lock
    public void notify(Notifier notifier) {
        Object key = notifier.getNotifiedKey();
        Queue<WaitingOp> q = mapWaitingOps.get(key);
        if (q == null) return;
        WaitingOp so = q.peek();
        while (so != null) {
            if (so.isValid()) {
                if (so.expired()) {
                    // expired
                    so.expire();
                } else {
                    if (so.shouldWait()) {
                        return;
                    }
                    waitingOpProcessor.processUnderExistingLock(so.getOperation());
                }
                so.setValid(false);
            }
            q.poll(); // consume
            so = q.peek();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SchedulingService{");
        sb.append("delayQueue=" + delayQueue.size());
        sb.append(" \n[");
        for (Queue<WaitingOp> ScheduledOps : mapWaitingOps.values()) {
            sb.append("\t");
            sb.append(ScheduledOps.size() + ", ");
        }
        sb.append("]\n}");
        return sb.toString();
    }

    public static void main(String[] args) {
        final WaitNotifyService ss = new WaitNotifyService(new WaitingOpProcessor() {
            public void process(WaitingOp so) {
            }

            public void processUnderExistingLock(Operation operation) {
            }
        });
        final Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                int count = 5000;
                for (int i = 0; i < count; i++) {
                    ss.wait(new WaitSupport() {
                        public Object getWaitKey() {
                            return "a";
                        }

                        public boolean shouldWait() {
                            return false;
                        }

                        public long getWaitTimeoutMillis() {
                            return 1200;
                        }

                        public void onWaitExpire() {
                            Queue<WaitingOp> q = ss.getScheduleQueue(getWaitKey());
                            q.remove(this);
                            System.out.println("invalid");
                        }
                    });
                }
                System.out.println("offered " + count);
                System.out.println(ss);
            }
        }, 0, 1000);
    }

    private Queue<WaitingOp> getScheduleQueue(Object scheduleQueueKey) {
        return mapWaitingOps.get(scheduleQueueKey);
    }

    interface WaitingOpProcessor {
        void process(WaitingOp so) throws Exception;

        void processUnderExistingLock(Operation operation);
    }

    public void shutdown() {
        expirationTask.cancel(true);
        esExpirationTaskExecutor.shutdown();
    }

    static class WaitingOp extends AbstractOperation implements Delayed {
        final Queue<WaitingOp> queue;
        final Operation op;
        final WaitSupport so;
        final long expirationTime;
        volatile boolean valid = true;

        WaitingOp(Queue<WaitingOp> queue, WaitSupport so) {
            this.op = (Operation) so;
            this.so = so;
            this.queue = queue;
            this.expirationTime = so.getWaitTimeoutMillis() < 0 ? -1 : System.currentTimeMillis() + so.getWaitTimeoutMillis();
        }

        public Operation getOperation() {
            return op;
        }

        public void setValid(boolean valid) {
            this.valid = valid;
        }

        public boolean isValid() {
            return valid;
        }

        public boolean expired() {
            return expirationTime != -1 && System.currentTimeMillis() >= expirationTime;
        }

        public boolean shouldWait() {
            return so.shouldWait();
        }

        public long getDelay(TimeUnit unit) {
            return unit.convert(expirationTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        public int compareTo(Delayed other) {
            if (other == this) // compare zero ONLY if same object
                return 0;
            long d = (getDelay(TimeUnit.NANOSECONDS) -
                    other.getDelay(TimeUnit.NANOSECONDS));
            return (d == 0) ? 0 : ((d < 0) ? -1 : 1);
        }

        @Override
        public void run() throws Exception {
            if (valid) {
                so.onWaitExpire();
                queue.remove(this);
            }
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }

        @Override
        public String getServiceName() {
            return op.getServiceName();
        }

        public void expire() {
            so.onWaitExpire();
        }

        public String toString() {
            return "W_" + Integer.toHexString(op.hashCode());
        }
    }
}
