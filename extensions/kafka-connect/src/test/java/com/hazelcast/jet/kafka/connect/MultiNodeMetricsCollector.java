/*
 * Copyright 2024 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.kafka.connect;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.hazelcast.jet.core.JetTestSupport.getNode;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class MultiNodeMetricsCollector<T extends MetricsCollector> {

    private final ScheduledExecutorService scheduler;
    private final T collector;

    MultiNodeMetricsCollector(HazelcastInstance[] instances, T collector) {
        this.scheduler = Executors.newScheduledThreadPool(instances.length);

        for (var inst : instances) {
            var registry = getNode(inst).nodeEngine.getMetricsRegistry();
            scheduler.scheduleAtFixedRate(() -> registry.collect(collector), 20, 3, MILLISECONDS);
        }

        this.collector = collector;
    }

    T collector() {
        return collector;
    }

    public void close() {
        scheduler.shutdownNow();
    }
}
