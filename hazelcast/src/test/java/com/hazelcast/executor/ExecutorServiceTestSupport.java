/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.core.PartitionAware;
import com.hazelcast.test.HazelcastTestSupport;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

    static class BasicTestTask implements Callable<String>, Serializable {
        public static String RESULT = "Task completed";

        @Override public String call() {
            return RESULT;
        }
    }

    static class SleepingTask implements Callable<Boolean>, Serializable, PartitionAware {
        long sleepSeconds;

        public SleepingTask(long sleepSeconds) {
            this.sleepSeconds = sleepSeconds;
        }
        @Override
        public Boolean call() throws InterruptedException {
            Thread.sleep(TimeUnit.SECONDS.toMillis(sleepSeconds));
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
            Future future = instance.getExecutorService("NestedExecutorTask").submit(new BasicTestTask());
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

        @Override public void run() {
            if (!initializeCalled) {
                fail("setHazelcastInstance() was not called");
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            initializeCalled = true;
        }
    }
}
