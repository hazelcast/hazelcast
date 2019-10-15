/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MetricTaggerImplTest {

    @Test
    public void testMetricId_withPrefixAndId() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        MetricTaggerImpl tagger = new MetricTaggerImpl(registry, "prefix")
                .withIdTag("idTag", "idValue")
                .withMetricTag("metricValue");

        assertEquals("prefix[idValue].metricValue", tagger.metricId());
    }

    @Test
    public void testMetricId_withPrefixWithoutId() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        MetricTaggerImpl tagger = new MetricTaggerImpl(registry, "prefix")
                .withMetricTag("metricValue");

        assertEquals("prefix.metricValue", tagger.metricId());
    }

    @Test
    public void testMetricId_withoutPrefixWithId() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        MetricTaggerImpl tagger = new MetricTaggerImpl(registry, null)
                .withIdTag("idTag", "idValue")
                .withMetricTag("metricValue");

        assertEquals("[idValue].metricValue", tagger.metricId());
    }

    @Test
    public void testMetricId_withoutPrefixAndId() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        MetricTaggerImpl tagger = new MetricTaggerImpl(registry, null)
                .withMetricTag("metricValue");

        assertEquals("metricValue", tagger.metricId());
    }

    @Test
    public void testMetricName_withPrefixAndId() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        MetricTaggerImpl tagger = new MetricTaggerImpl(registry, "prefix")
                .withIdTag("idTag", "idValue")
                .withMetricTag("metricValue");

        assertEquals("[idTag=idValue,metric=prefix.metricValue]", tagger.metricName());
    }

    @Test
    public void testMetricName_withPrefixWithoutId() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        MetricTaggerImpl tagger = new MetricTaggerImpl(registry, "prefix")
                .withMetricTag("metricValue");

        assertEquals("[metric=prefix.metricValue]", tagger.metricName());
    }

    @Test
    public void testMetricName_withoutPrefixWithId() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        MetricTaggerImpl tagger = new MetricTaggerImpl(registry, null)
                .withIdTag("idTag", "idValue")
                .withMetricTag("metricValue");

        assertEquals("[idTag=idValue,metric=metricValue]", tagger.metricName());
    }

    @Test
    public void testMetricName_withoutPrefixAndId() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        MetricTaggerImpl tagger = new MetricTaggerImpl(registry, null)
                .withMetricTag("metricValue");

        assertEquals("[metric=metricValue]", tagger.metricName());
    }
}
