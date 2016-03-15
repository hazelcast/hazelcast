/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.executor;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.test.HazelcastTestSupport;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.fail;

public class ExecutorServiceTestSupport extends HazelcastTestSupport {

    IExecutorService createSingleNodeExecutorService(String name) {
        return createSingleNodeExecutorService(name, ExecutorConfig.DEFAULT_POOL_SIZE);
    }

    IExecutorService createSingleNodeExecutorService(String name, int poolSize) {
        final HazelcastInstance instance = createHazelcastInstance(
                new Config().addExecutorConfig(new ExecutorConfig(name, poolSize)));
        return instance.getExecutorService(name);
    }

    int findNextKeyForMember(HazelcastInstance instance, Member localMember) {
        int key = 0;
        while (!localMember.equals(instance.getPartitionService().getPartition(++key).getOwner())) ;
        return key;
    }

    InternalExecutionService getExecutionService(HazelcastInstance instance) {
        return getNode(instance).getNodeEngine().getExecutionService();
    }

    static class CountDownLatchAwaitingCallable
            implements Callable<String> {

        public static String RESULT = "Success";

        private final CountDownLatch latch;

        public CountDownLatchAwaitingCallable(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public String call()
                throws Exception {
            latch.await(30, TimeUnit.SECONDS);
            return RESULT;
        }
    }

    public static class CountingDownExecutionCallback<T> implements ExecutionCallback<T> {

        private final CountDownLatch latch;

        private final AtomicReference<Object> result = new AtomicReference<Object>();

        public CountingDownExecutionCallback(int count) {
            this.latch = new CountDownLatch(count);
        }

        public CountingDownExecutionCallback(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onResponse(T response) {
            if (!result.compareAndSet(null, response)) {
                System.out.println("New response received after result is set. Response: " + response + " Resuilt: " + result.get());
            }
            latch.countDown();
        }

        @Override
        public void onFailure(Throwable t) {
            if (!result.compareAndSet(null, t)) {
                System.out.println("Failure received after result is set. Failure: " + t + " Resuilt: " + result.get());
            }
            latch.countDown();
        }

        public CountDownLatch getLatch() {
            return latch;
        }

        public Object getResult() {
            return result.get();
        }
    }

    static class BasicTestCallable
            implements Callable<String>, Serializable, PartitionAware {
        public static String RESULT = "Task completed";

        @Override
        public String call() {
            return RESULT;
        }

        @Override
        public Object getPartitionKey() {
            return "key";
        }
    }

    static class SleepingTask
            implements Callable<Boolean>, Serializable, PartitionAware {
        long sleepSeconds;

        public SleepingTask(long sleepSeconds) {
            this.sleepSeconds = sleepSeconds;
        }

        @Override
        public Boolean call() throws InterruptedException {
            sleepAtLeastSeconds((int) sleepSeconds);
            return true;
        }

        @Override
        public Object getPartitionKey() {
            return "key";
        }
    }

    static class NestedExecutorTask implements Callable<String>, Serializable, HazelcastInstanceAware {
        private HazelcastInstance instance;

        @Override
        public String call() throws Exception {
            Future future = instance.getExecutorService("NestedExecutorTask").submit(new BasicTestCallable());
            return (String) future.get();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            instance = hazelcastInstance;
        }
    }

    static class MemberCheck implements Callable<Member>, Serializable, HazelcastInstanceAware {
        private Member localMember;

        @Override
        public Member call() throws Exception {
            return localMember;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            localMember = hazelcastInstance.getCluster().getLocalMember();
        }
    }

    static class FailingTestTask implements Callable<String>, Serializable {
        @Override
        public String call() throws Exception {
            throw new IllegalStateException();
        }
    }

    static class HazelcastInstanceAwareRunnable implements Runnable, HazelcastInstanceAware, Serializable {
        private transient boolean initializeCalled;

        @Override
        public void run() {
            if (!initializeCalled) {
                fail("setHazelcastInstance() was not called");
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            initializeCalled = true;
        }
    }

    static class IncrementAtomicLongIfMemberUUIDNotMatchRunnable implements Runnable, Serializable, HazelcastInstanceAware {

        private final String uuid;
        private final String name;

        private HazelcastInstance instance;

        public IncrementAtomicLongIfMemberUUIDNotMatchRunnable(String uuid, String name) {
            this.uuid = uuid;
            this.name = name;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public void run() {
            if (!instance.getCluster().getLocalMember().getUuid().equals(uuid)) {
                instance.getAtomicLong(name).incrementAndGet();
            }
        }
    }

    static class NullResponseCountingCallback<T> implements ExecutionCallback<T> {

        private final AtomicInteger nullResponseCount = new AtomicInteger(0);

        private final CountDownLatch responseLatch;

        public NullResponseCountingCallback(int count) {
            this.responseLatch = new CountDownLatch(count);
        }

        @Override
        public void onResponse(T response) {
            if (response == null) {
                nullResponseCount.incrementAndGet();
            }

            responseLatch.countDown();
        }

        @Override
        public void onFailure(Throwable t) {
            System.out.println("Exception received: " + t);
        }

        public int getNullResponseCount() {
            return nullResponseCount.get();
        }

        public boolean awaitResponseLatch(int seconds)
                throws InterruptedException {
            return responseLatch.await(seconds, TimeUnit.SECONDS);
        }

        public CountDownLatch getResponseLatch() {
            return responseLatch;
        }
    }

    static class ResponseCountingMultiExecutionCallback implements MultiExecutionCallback {

        private final AtomicInteger count = new AtomicInteger();

        private final CountDownLatch latch;

        public ResponseCountingMultiExecutionCallback(int count) {
            this.latch = new CountDownLatch(count);
        }

        public void onResponse(Member member, Object value) {
            count.incrementAndGet();
        }

        public void onComplete(Map<Member, Object> values) {
            latch.countDown();
        }

        public int getCount() {
            return count.get();
        }

        public CountDownLatch getLatch() {
            return latch;
        }
    }

    ;

    static class BooleanSuccessResponseCountingCallback
            implements ExecutionCallback<Boolean> {

        private final AtomicInteger successResponseCount = new AtomicInteger(0);

        private final CountDownLatch responseLatch;

        public BooleanSuccessResponseCountingCallback(int count) {
            this.responseLatch = new CountDownLatch(count);
        }

        @Override
        public void onResponse(Boolean response) {
            if (response) {
                successResponseCount.incrementAndGet();
            }
            responseLatch.countDown();
        }

        @Override
        public void onFailure(Throwable t) {

        }

        public int getSuccessResponseCount() {
            return successResponseCount.get();
        }

        public CountDownLatch getResponseLatch() {
            return responseLatch;
        }
    }

    static class IncrementAtomicLongRunnable implements Runnable, Serializable, HazelcastInstanceAware {

        private final String name;

        private HazelcastInstance instance;

        public IncrementAtomicLongRunnable(String name) {
            this.name = name;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public void run() {
            instance.getAtomicLong(name).incrementAndGet();
        }
    }

    static class IncrementAtomicLongCallable implements Callable<Long>, Serializable, HazelcastInstanceAware {

        private final String name;

        private HazelcastInstance instance;

        public IncrementAtomicLongCallable(String name) {
            this.name = name;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

        public void run() {
            instance.getAtomicLong(name).incrementAndGet();
        }

        @Override
        public Long call()
                throws Exception {
            return instance.getAtomicLong(name).incrementAndGet();
        }
    }

    static class MemberUUIDCheckCallable implements Callable<Boolean>, HazelcastInstanceAware, Serializable {

        private final String uuid;

        public MemberUUIDCheckCallable(String uuid) {
            this.uuid = uuid;
        }

        private HazelcastInstance instance;

        @Override
        public Boolean call()
                throws Exception {
            return instance.getCluster().getLocalMember().getUuid().equals(uuid);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }
    }

    public static class ResultSettingRunnable implements Runnable, HazelcastInstanceAware, Serializable {

        private final String name;

        public ResultSettingRunnable(String name) {
            this.name = name;
        }

        private transient HazelcastInstance instance;

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public void run() {
            final Member member = instance.getCluster().getLocalMember();
            instance.getMap(name).put(member, true);
        }

    }

    public static class LocalMemberReturningCallable implements Callable<Member>, HazelcastInstanceAware, Serializable {


        private transient HazelcastInstance instance;

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public Member call() {
            return instance.getCluster().getLocalMember();
        }

    }

    public static class ResultHoldingMultiExecutionCallback implements MultiExecutionCallback {

        private volatile Map<Member, Object> results;

        public Map<Member, Object> getResults() {
            return results;
        }

        @Override
        public void onResponse(Member member, Object value) {
            // ignored
        }

        @Override
        public void onComplete(Map<Member, Object> values) {
            this.results = values;
        }
    }
}
