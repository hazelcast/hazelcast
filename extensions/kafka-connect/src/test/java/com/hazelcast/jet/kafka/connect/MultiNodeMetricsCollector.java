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
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.core.JetTestSupport.getNode;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class MultiNodeMetricsCollector<T extends MetricsCollector> implements AutoCloseable {

    private final ScheduledExecutorService scheduler;
    private final T collector;

    MultiNodeMetricsCollector(HazelcastInstance[] instances, T collector) {
        this.scheduler = Executors.newScheduledThreadPool(instances.length);
        this.collector = collector;

        for (var inst : instances) {
            var registry = getNode(inst).nodeEngine.getMetricsRegistry();
            // Schedule immediately
            scheduler.scheduleAtFixedRate(() -> registry.collect(collector), 0, 3, MILLISECONDS);
        }
    }

    T collector() {
        return collector;
    }

    @Override
    public void close() {
        try {
            scheduler.shutdown();
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                // Cancel currently executing tasks
                scheduler.shutdownNow();
            }
        } catch (InterruptedException ignored) {
        }
    }
}
