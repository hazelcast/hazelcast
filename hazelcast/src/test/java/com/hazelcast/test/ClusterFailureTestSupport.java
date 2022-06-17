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

    private interface ClusterFailure {
        void initialize(int initialClusterSize, Config config);

        void fail();

        void cleanUp();

        HazelcastInstance createClient(ClientConfig clientConfig);

    }

    public abstract static class SingleFailingInstanceClusterFailure implements ClusterFailure {
        protected HazelcastInstance[] hazelcastInstances;
        protected HazelcastInstance failingInstance;
        protected TestHazelcastFactory factory = new TestHazelcastFactory();
        protected HazelcastInstance client;
        protected int initialClusterSize;
        protected Config config;

        @Override
        public void initialize(int initialClusterSize, Config config) {
            this.initialClusterSize = initialClusterSize;
            this.config = config;

            hazelcastInstances = factory.newInstances(config, initialClusterSize);
            HazelcastTestSupport.assertClusterSizeEventually(initialClusterSize, hazelcastInstances[0]);
            failingInstance = factory.newHazelcastInstance(config);
            HazelcastTestSupport.assertClusterSizeEventually(initialClusterSize + 1, hazelcastInstances[0]);
            HazelcastTestSupport.waitAllForSafeState(hazelcastInstances);
            HazelcastTestSupport.waitAllForSafeState(failingInstance);
            client = null;
        }

        @Override
        public HazelcastInstance createClient(ClientConfig clientConfig) {
            client = factory.newHazelcastClient(clientConfig);
            return client;
        }

        @Override
        public void cleanUp() {
            Arrays.stream(hazelcastInstances).forEach(instance -> instance.getLifecycleService().terminate());
            client.getLifecycleService().terminate();
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }

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
            HazelcastTestSupport.assertClusterSizeEventually(initialClusterSize, hazelcastInstances[0]);
        }

        @Override
        public void cleanUp() {
            super.cleanUp();
            failingInstance.getLifecycleService().terminate();
        }
    }

    public static class NodeReplacementClusterFailure extends SingleFailingInstanceClusterFailure {
        private HazelcastInstance replacementInstance;

        @Override
        public void fail() {
            failingInstance.getLifecycleService().terminate();
            HazelcastTestSupport.assertClusterSizeEventually(initialClusterSize, hazelcastInstances[0]);
            replacementInstance = factory.newHazelcastInstance(config);
            HazelcastTestSupport.assertClusterSizeEventually(initialClusterSize + 1, hazelcastInstances[0]);
        }

        @Override
        public void cleanUp() {
            super.cleanUp();
            replacementInstance.getLifecycleService().terminate();
        }
    }

    public static class NodeTerminationClusterFailure extends SingleFailingInstanceClusterFailure {
        @Override
        public void fail() {
            failingInstance.getLifecycleService().terminate();
            HazelcastTestSupport.assertClusterSizeEventually(initialClusterSize, hazelcastInstances[0]);
        }
    }

    public static class NodeShutdownClusterFailure extends SingleFailingInstanceClusterFailure {
        @Override
        public void fail() {
            failingInstance.shutdown();
            HazelcastTestSupport.assertClusterSizeEventually(initialClusterSize, hazelcastInstances[0]);
        }
    }
}
