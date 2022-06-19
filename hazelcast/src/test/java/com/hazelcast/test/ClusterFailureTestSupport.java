/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import java.util.Arrays;

public final class ClusterFailureTestSupport {
    private ClusterFailureTestSupport() {
    }

    public abstract static class SingleFailingInstanceClusterFailure {
        protected HazelcastInstance[] hazelcastInstances;
        protected HazelcastInstance failingInstance;
        protected TestHazelcastFactory factory = new TestHazelcastFactory();
        protected HazelcastInstance client;
        protected int initialClusterMembersCount;
        protected Config config;

        public void initialize(int initialClusterMembersCount, Config config) {
            this.initialClusterMembersCount = initialClusterMembersCount;
            this.config = config;

            hazelcastInstances = factory.newInstances(config, initialClusterMembersCount);
            HazelcastTestSupport.assertClusterSizeEventually(initialClusterMembersCount, hazelcastInstances[0]);
            failingInstance = factory.newHazelcastInstance(config);
            HazelcastTestSupport.assertClusterSizeEventually(initialClusterMembersCount + 1, hazelcastInstances[0]);
            HazelcastTestSupport.waitAllForSafeState(hazelcastInstances);
            HazelcastTestSupport.waitAllForSafeState(failingInstance);
            client = null;
        }

        public HazelcastInstance createClient(ClientConfig clientConfig) {
            client = factory.newHazelcastClient(clientConfig);
            return client;
        }

        public void cleanUp() {
            Arrays.stream(hazelcastInstances).forEach(instance -> instance.getLifecycleService().terminate());
            client.getLifecycleService().terminate();
        }

        public String toString() {
            return this.getClass().getSimpleName();
        }

        public abstract void fail();

        public abstract void recover();

        public HazelcastInstance getFailingInstance() {
            return failingInstance;
        }

        public HazelcastInstance getNotFailingInstance() {
            return hazelcastInstances[0];
        }
    }

    public static class NetworkProblemClusterFailure extends SingleFailingInstanceClusterFailure {
        @Override
        public void fail() {
            for (HazelcastInstance hazelcastInstance : hazelcastInstances) {
                HazelcastTestSupport.closeConnectionBetween(hazelcastInstance, failingInstance);
            }
            HazelcastTestSupport.assertClusterSizeEventually(initialClusterMembersCount, hazelcastInstances[0]);
            HazelcastTestSupport.waitAllForSafeState(hazelcastInstances);
        }

        @Override
        public void cleanUp() {
            super.cleanUp();
            failingInstance.getLifecycleService().terminate();
        }

        @Override
        public void recover() {
            failingInstance.getLifecycleService().terminate();
            failingInstance = factory.newHazelcastInstance(config);
            HazelcastTestSupport.assertClusterSizeEventually(initialClusterMembersCount + 1, hazelcastInstances[0]);
            HazelcastTestSupport.waitAllForSafeState(hazelcastInstances);
            HazelcastTestSupport.waitAllForSafeState(failingInstance);
        }
    }

    public static class NodeReplacementClusterFailure extends SingleFailingInstanceClusterFailure {
        private HazelcastInstance replacementInstance;

        @Override
        public void fail() {
            failingInstance.getLifecycleService().terminate();
            HazelcastTestSupport.assertClusterSizeEventually(initialClusterMembersCount, hazelcastInstances[0]);
            replacementInstance = factory.newHazelcastInstance(config);
            HazelcastTestSupport.assertClusterSizeEventually(initialClusterMembersCount + 1, hazelcastInstances[0]);
            HazelcastTestSupport.waitAllForSafeState(hazelcastInstances);
            HazelcastTestSupport.waitAllForSafeState(replacementInstance);
        }

        @Override
        public void cleanUp() {
            super.cleanUp();
            replacementInstance.getLifecycleService().terminate();
        }

        @Override
        public void recover() {
            failingInstance = replacementInstance;
            replacementInstance = null;
        }
    }

    public static class NodeTerminationClusterFailure extends SingleFailingInstanceClusterFailure {
        @Override
        public void fail() {
            failingInstance.getLifecycleService().terminate();
            HazelcastTestSupport.assertClusterSizeEventually(initialClusterMembersCount, hazelcastInstances[0]);
            HazelcastTestSupport.waitAllForSafeState(hazelcastInstances);
        }

        @Override
        public void recover() {
            failingInstance = factory.newHazelcastInstance(config);
            HazelcastTestSupport.assertClusterSizeEventually(initialClusterMembersCount + 1, hazelcastInstances[0]);
            HazelcastTestSupport.waitAllForSafeState(hazelcastInstances);
            HazelcastTestSupport.waitAllForSafeState(failingInstance);
        }
    }

    public static class NodeShutdownClusterFailure extends SingleFailingInstanceClusterFailure {
        @Override
        public void fail() {
            failingInstance.shutdown();
            HazelcastTestSupport.assertClusterSizeEventually(initialClusterMembersCount, hazelcastInstances[0]);
            HazelcastTestSupport.waitAllForSafeState(hazelcastInstances);
        }

        @Override
        public void recover() {
            failingInstance = factory.newHazelcastInstance(config);
            HazelcastTestSupport.assertClusterSizeEventually(initialClusterMembersCount + 1, hazelcastInstances[0]);
            HazelcastTestSupport.waitAllForSafeState(hazelcastInstances);
            HazelcastTestSupport.waitAllForSafeState(failingInstance);
        }
    }

    public abstract static class MultipleFailingInstanceClusterFailure {
        protected HazelcastInstance[] hazelcastInstances;
        protected HazelcastInstance[] failingInstances;
        protected TestHazelcastFactory factory;
        protected HazelcastInstance client;
        protected int initialClusterMembersCount;
        protected int currentSize;
        protected int lastLivingFailingInstanceIndex;
        protected Config config;

        public void initialize(int initialClusterMembersCount, int initialFailingMembersCount, Config config) {
            this.initialClusterMembersCount = initialClusterMembersCount;
            this.config = config;
            factory = new TestHazelcastFactory();
            hazelcastInstances = factory.newInstances(config, initialClusterMembersCount);
            HazelcastTestSupport.assertClusterSizeEventually(initialClusterMembersCount, hazelcastInstances[0]);
            failingInstances = factory.newInstances(config, initialFailingMembersCount);
            currentSize = initialClusterMembersCount + initialFailingMembersCount;
            lastLivingFailingInstanceIndex = initialFailingMembersCount - 1;
            HazelcastTestSupport.assertClusterSizeEventually(currentSize, hazelcastInstances[0]);
            HazelcastTestSupport.waitAllForSafeState(hazelcastInstances);
            HazelcastTestSupport.waitAllForSafeState(failingInstances);
            client = null;
        }

        public HazelcastInstance createClient(ClientConfig clientConfig) {
            client = factory.newHazelcastClient(clientConfig);
            return client;
        }

        public void cleanUp() {
            Arrays.stream(hazelcastInstances).forEach(instance -> instance.getLifecycleService().terminate());
            client.getLifecycleService().terminate();
            factory.terminateAll();
        }

        public String toString() {
            return this.getClass().getSimpleName();
        }

        public abstract void fail();

        public HazelcastInstance getFailingInstance() {
            return failingInstances[0];
        }

        public HazelcastInstance getNotFailingInstance() {
            return hazelcastInstances[0];
        }
    }

    public static class NodeShutdownClusterMultipleFailure extends MultipleFailingInstanceClusterFailure {
        @Override
        public void fail() {
            failingInstances[lastLivingFailingInstanceIndex--].shutdown();
            currentSize--;
            HazelcastTestSupport.assertClusterSizeEventually(currentSize, hazelcastInstances[0]);
        }
    }
}
