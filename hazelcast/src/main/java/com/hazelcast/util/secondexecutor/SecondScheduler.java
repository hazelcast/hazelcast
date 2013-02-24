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

package com.hazelcast.util.secondexecutor;

import java.util.Map;
import java.util.concurrent.*;

class SecondScheduler implements SecondExecutorService {
    final ConcurrentMap<Object, Integer> keySeconds = new ConcurrentHashMap<Object, Integer>(1000);
    final ConcurrentMap<Integer, ConcurrentMap<Object, Object>> secondObjects = new ConcurrentHashMap<Integer, ConcurrentMap<Object, Object>>(1000);
    final ScheduledExecutorService ses;
    private static final long initialTimeMillis = System.currentTimeMillis();
    final SecondTaskFactory secondTaskFactory;
    final boolean bulk;

    public SecondScheduler(ScheduledExecutorService ses, SecondTaskFactory secondTaskFactory) {
        this.ses = ses;
        this.secondTaskFactory = secondTaskFactory;
        bulk = (secondTaskFactory instanceof SecondBulkTaskFactory);
    }

    interface SecondTaskFactory {
        SecondTask newSecondTask();
    }

    public boolean schedule(long delayMillis, Object key, Object object) {
        if (bulk) {
            return scheduleIfNew(delayMillis, key, object);
        } else {
            return scheduleEntry(delayMillis, key, object);
        }
    }

    boolean scheduleEntry(long delayMillis, Object key, Object object) {
        final int delaySeconds = ceilToSecond(delayMillis);
        final Integer newSecond = findRelativeSecond(delayMillis);
        final Integer existingSecond = keySeconds.put(key, newSecond);
        if (existingSecond != null) {
            if (existingSecond.equals(newSecond)) return false;
            removeKeyFromSecond(key, existingSecond);
        }
        doSchedule(key, object, newSecond, delaySeconds);
        return true;
    }

    private int findRelativeSecond(long delayMillis) {
        long now = System.currentTimeMillis();
        long d = (now + delayMillis - initialTimeMillis);
        return ceilToSecond(d);
    }

    private int ceilToSecond(long delayMillis) {
        return (int) Math.ceil(delayMillis / 1000D);
    }

    boolean scheduleIfNew(long delayMillis, Object key, Object Object) {
        final int delaySeconds = ceilToSecond(delayMillis);
        final Integer newSecond = findRelativeSecond(delayMillis);
        if (keySeconds.putIfAbsent(key, newSecond) != null) return false;
        doSchedule(key, Object, newSecond, delaySeconds);
        return true;
    }

    private void doSchedule(Object key, Object Object, Integer newSecond, int delaySeconds) {
        ConcurrentMap<Object, Object> scheduledKeys = secondObjects.get(newSecond);
        if (scheduledKeys == null) {
            scheduledKeys = new ConcurrentHashMap<Object, Object>(10);
            ConcurrentMap<Object, Object> existingScheduleKeys = secondObjects.putIfAbsent(newSecond, scheduledKeys);
            if (existingScheduleKeys != null) {
                scheduledKeys = existingScheduleKeys;
            } else {
                // we created the second
                // so we will schedule its execution
                execute(newSecond, delaySeconds);
            }
        }
        scheduledKeys.put(key, Object);
    }

    private void removeKeyFromSecond(Object key, Integer existingSecond) {
        ConcurrentMap<Object, Object> scheduledKeys = secondObjects.get(existingSecond);
        if (scheduledKeys != null) {
            scheduledKeys.remove(key);
        }
    }

    void execute(final Integer second, final int delaySecond) {
        ses.schedule(new Runnable() {
            public void run() {
                final ConcurrentMap<Object, Object> scheduledKeys = secondObjects.remove(second);
                final SecondTask secondTask = secondTaskFactory.newSecondTask();
                try {
                    if (bulk) {
                        final SecondBulkTask sbe = (SecondBulkTask) secondTask;
                        for (Object o : scheduledKeys.keySet()) {
                            keySeconds.remove(o);
                        }
                        sbe.executeAll(SecondScheduler.this, scheduledKeys, delaySecond);
                    } else {
                        final SecondEntryTask see = (SecondEntryTask) secondTask;
                        for (Map.Entry<Object, Object> object : scheduledKeys.entrySet()) {
                            final Object scheduleKey = object.getKey();
                            if (keySeconds.remove(scheduleKey, second)) {
                                try {
                                    see.executeEntry(SecondScheduler.this, object, delaySecond);
                                } catch (Exception ignored) {
                                }
                            }
                        }
                    }
                } catch (Throwable ignored) {
                    ignored.printStackTrace();
                } finally {
                    secondTask.endSecond();
                }
            }
        }, delaySecond, TimeUnit.SECONDS);
        System.out.println("Scheduled for " + second);
    }

    @Override
    public String toString() {
        return "KeyScheduler{" +
                "keySeconds=" + keySeconds.size() +
                ", secondObjects=" + secondObjects.keySet() +
                '}';
    }

    public static void main(String[] args) throws Exception {
        SecondTaskFactory secondTaskFactory = new SecondTaskFactory() {
            public SecondTask newSecondTask() {
//                return new SecondEntryExecutor() {
//                    public void executeEntry(Map.Entry entry) {
//                        System.out.println(entry.getValue());
//                    }
//
//                    public void endSecond() {
//                    }
//                };
                return new SecondBulkTask() {
                    public void executeAll(SecondExecutorService ses, ConcurrentMap<Object, Object> entries, int delaySecond) {
                        for (Object o : entries.values()) {
                            System.out.println(o);
                        }
                    }

                    public void endSecond() {
                    }
                };
            }
        };
        final ScheduledExecutorService ses = Executors.newScheduledThreadPool(10);
        final SecondScheduler secondScheduler = new SecondScheduler(ses, secondTaskFactory);
        new Thread() {
            @Override
            public synchronized void run() {
                while (true) {
                    System.out.println(secondScheduler);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();
        while (true) {
            for (int i = 0; i < 10; i++) {
                schedule(true, secondScheduler, i, 100);
            }
        }
    }

    public static void main2(String[] args) throws Exception {
        SecondTaskFactory secondTaskFactory = new SecondTaskFactory() {
            public SecondTask newSecondTask() {
//                return new SecondEntryExecutor() {
//                    public void executeEntry(Map.Entry entry) {
//                        System.out.println(entry.getValue());
//                    }
//
//                    public void endSecond() {
//                    }
//                };
                return new SecondBulkTask() {
                    public void executeAll(SecondExecutorService ses, ConcurrentMap<Object, Object> entries, int delaySecond) {
                        for (Object o : entries.values()) {
                            System.out.println(o);
                        }
                    }

                    public void endSecond() {
                    }
                };
            }
        };
        final ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
        SecondScheduler secondScheduler = new SecondScheduler(ses, secondTaskFactory);
        for (int i = 0; i < 10; i++) {
            schedule(true, secondScheduler, i, 100);
            schedule(true, secondScheduler, i, 100);
            schedule(true, secondScheduler, i, 100);
        }
        for (int i = 10; i < 20; i++) {
            schedule(true, secondScheduler, i, 3330);
            schedule(true, secondScheduler, i, 4330);
            schedule(true, secondScheduler, i, 5330);
        }
        //        for (int i = 20; i < 30; i++) {
        //            schedule(false, keyScheduler, i, 5330);
        //            schedule(false, keyScheduler, i, 6330);
        //            schedule(false, keyScheduler, i, 7330);
        //
        //        }
        while (true) {
            System.out.println(secondScheduler);
            Thread.sleep(1000);
        }
    }

    private static void schedule(boolean update, SecondScheduler secondScheduler, final Object key, final long millis) {
        if (update) {
            secondScheduler.schedule(millis, key, "Update-" + key);
        } else {
            secondScheduler.scheduleIfNew(millis, key, "IfNew-" + key);
        }
    }
}
