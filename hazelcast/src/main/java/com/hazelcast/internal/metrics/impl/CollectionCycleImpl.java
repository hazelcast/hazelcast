/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.CollectionCycle;
import com.hazelcast.internal.metrics.MetricSource;
import com.hazelcast.internal.metrics.MetricsCollector;
import com.hazelcast.internal.metrics.ProbeLevel;

final class CollectionCycleImpl implements CollectionCycle {
    private final ProbeLevel probeLevel;
    private final MetricsRegistryImpl metricsRegistry;
    private final MetricsCollector collector;

    private final TagsImpl tags = new TagsImpl();

    CollectionCycleImpl(MetricsRegistryImpl metricsRegistry, MetricsCollector collector, ProbeLevel probeLevel) {
        this.metricsRegistry = metricsRegistry;
        this.collector = collector;
        this.probeLevel = probeLevel;
    }

    @Override
    public ProbeLevel probeLevel() {
        return probeLevel;
    }

    @Override
    public Tags newTags() {
        tags.reset();
        return tags;
    }

    @Override
    public void collect(Object source) {
       // System.out.println("                      " + source);
    }

    @Override
    public void collectLong(long v) {
        tags.completeTagBuffer();
        collector.collectLong(tags.tagBuffer, v);
    }

    @Override
    public void collectDouble(double v) {
        tags.completeTagBuffer();
        collector.collectDouble(tags.tagBuffer, v);
    }

    @Override
    public void collect(final String namespace, Object source) {
        SourceMetadata sourceMetadata = metricsRegistry.loadSourceMetadata(source.getClass());
        for (AbstractProbe probe : sourceMetadata.probes) {
            probe.collect(this, source);
        }

        if (source instanceof MetricSource) {
            ((MetricSource) source).collectMetrics(this);
        }
    }

    private static class TagsImpl implements Tags {
        private final StringBuffer metric = new StringBuffer();
        private final StringBuffer tagBuffer = new StringBuffer();
        private boolean tagsAdded;

        @Override
        public Tags add(String tag, String value) {
            onTagAdded();
            tagBuffer.append(tagBuffer).append('=').append(value);
            return this;
        }

        @Override
        public Tags add(String tag, int value) {
            onTagAdded();
            tagBuffer.append(tagBuffer).append('=').append(value);
            return this;
        }

        private void onTagAdded() {
            if (tagsAdded) {
                tagBuffer.append(',');
            } else {
                tagsAdded = true;
            }
        }

        @Override
        public Tags metricPrefix(String prefix) {
            metric.append(prefix);
            return this;
        }

        @Override
        public Tags metricSuffix(String suffix) {
            metric.append(".").append(suffix);
            return this;
        }

        @Override
        public Tags metric(String name) {
            this.metric.append(name);
            return this;
        }

        private void completeTagBuffer() {
            if (metric.length() == 0) {
                return;
            }

            if (tagsAdded) {
                tagBuffer.append(',');
            }

            tagBuffer.append("metric=").append(metric);
        }

        void reset() {
            tagsAdded = false;
            tagBuffer.setLength(0);
            metric.setLength(0);
        }
    }
}
