/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
            assertClusterSizeEventually(initialClusterMembersCount, null);
            failingInstance = factory.newHazelcastInstance(config);
            assertClusterSizeEventually(initialClusterMembersCount + 1, failingInstance);
            HazelcastTestSupport.waitAllForSafeState(hazelcastInstances);
            HazelcastTestSupport.waitAllForSafeState(failingInstance);
            client = null;
        }

        public HazelcastInstance createClient(ClientConfig clientConfig) {
            client = factory.newHazelcastClient(clientConfig);
            return client;
        }

        public void cleanUp() {
            client.getLifecycleService().terminate();
            factory.terminateAll();
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

        protected void assertClusterSizeEventually(int size, HazelcastInstance additionalInstance) {
            for (int i = 0; i < hazelcastInstances.length; i++) {
                HazelcastInstance instance = hazelcastInstances[i];
                HazelcastTestSupport.assertClusterSizeEventually(size, instance);
            }
            if (additionalInstance != null) {
                HazelcastTestSupport.assertClusterSizeEventually(size, additionalInstance);
            }
        }
    }

    public static class NetworkProblemClusterFailure extends SingleFailingInstanceClusterFailure {
        @Override
        public void fail() {
            for (HazelcastInstance hazelcastInstance : hazelcastInstances) {
                HazelcastTestSupport.closeConnectionBetween(hazelcastInstance, failingInstance);
            }
            assertClusterSizeEventually(initialClusterMembersCount, null);
            HazelcastTestSupport.waitAllForSafeState(hazelcastInstances);
        }

        @Override
        public void recover() {
            failingInstance.getLifecycleService().terminate();
            failingInstance = factory.newHazelcastInstance(config);
            assertClusterSizeEventually(initialClusterMembersCount + 1, failingInstance);
            HazelcastTestSupport.waitAllForSafeState(hazelcastInstances);
            HazelcastTestSupport.waitAllForSafeState(failingInstance);
        }
    }

    public static class NodeReplacementClusterFailure extends SingleFailingInstanceClusterFailure {
        private HazelcastInstance replacementInstance;

        @Override
        public void fail() {
            failingInstance.getLifecycleService().terminate();
            assertClusterSizeEventually(initialClusterMembersCount, null);
            replacementInstance = factory.newHazelcastInstance(config);
            assertClusterSizeEventually(initialClusterMembersCount + 1, replacementInstance);
            HazelcastTestSupport.waitAllForSafeState(hazelcastInstances);
            HazelcastTestSupport.waitAllForSafeState(replacementInstance);
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
            assertClusterSizeEventually(initialClusterMembersCount, null);
            HazelcastTestSupport.waitAllForSafeState(hazelcastInstances);
        }

        @Override
        public void recover() {
            failingInstance = factory.newHazelcastInstance(config);
            assertClusterSizeEventually(initialClusterMembersCount + 1, failingInstance);
            HazelcastTestSupport.waitAllForSafeState(hazelcastInstances);
            HazelcastTestSupport.waitAllForSafeState(failingInstance);
        }
    }

    public static class NodeShutdownClusterFailure extends SingleFailingInstanceClusterFailure {
        @Override
        public void fail() {
            failingInstance.shutdown();
            assertClusterSizeEventually(initialClusterMembersCount, null);
            HazelcastTestSupport.waitAllForSafeState(hazelcastInstances);
        }

        @Override
        public void recover() {
            failingInstance = factory.newHazelcastInstance(config);
            assertClusterSizeEventually(initialClusterMembersCount + 1, failingInstance);
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
            assertClusterSizeEventually(initialClusterMembersCount);
            failingInstances = factory.newInstances(config, initialFailingMembersCount);
            currentSize = initialClusterMembersCount + initialFailingMembersCount;
            lastLivingFailingInstanceIndex = initialFailingMembersCount - 1;
            assertClusterSizeEventually(currentSize);
            HazelcastTestSupport.waitAllForSafeState(hazelcastInstances);
            HazelcastTestSupport.waitAllForSafeState(failingInstances);
            client = null;
        }

        public HazelcastInstance createClient(ClientConfig clientConfig) {
            client = factory.newHazelcastClient(clientConfig);
            return client;
        }

        public void cleanUp() {
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

        protected void assertClusterSizeEventually(int size) {
            for (int i = 0; i < hazelcastInstances.length; i++) {
                HazelcastInstance instance = hazelcastInstances[i];
                HazelcastTestSupport.assertClusterSizeEventually(size, instance);
            }
            if (failingInstances != null) {
                for (int i = 0; i <= lastLivingFailingInstanceIndex; i++) {
                    HazelcastInstance instance = failingInstances[i];
                    HazelcastTestSupport.assertClusterSizeEventually(size, instance);
                }
            }
        }
    }
}
