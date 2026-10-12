/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPMapConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@QuickTest
public class CPMapServiceMetricTest {

    private NodeEngine nodeEngine;

    public record MetricEntry(MetricDescriptor descriptor, long value) {
    }

    public static class DummyCollector implements MetricsCollectionContext {
        List<MetricEntry> entries = new ArrayList<>();

        @Override
        public void collect(MetricDescriptor descriptor, Object source) {
        }

        @Override
        public void collect(MetricDescriptor descriptor, String name, ProbeLevel level, ProbeUnit unit, long value) {
        }

        @Override
        public void collect(MetricDescriptor descriptor, String name, ProbeLevel level, ProbeUnit unit, double value) {
        }

        @Override
        public void collect(MetricDescriptor descriptor, long value) {
            entries.add(new MetricEntry(descriptor, value));
        }

        @Override
        public void collect(MetricDescriptor descriptor, double value) {
        }

        public List<MetricEntry> getEntries() {
            return entries;
        }
    }

    public static class DummyMetricDescriptor implements MetricDescriptor {
        private String prefix;
        private String discrimatorValue;
        private String metric;
        private ProbeUnit unit;
        private final Map<String, String> tags = new HashMap<>();

        public DummyMetricDescriptor() {
            this("");
        }

        DummyMetricDescriptor(String prefix) {
            this.prefix = prefix;
        }

        private DummyMetricDescriptor(DummyMetricDescriptor other) {
            this.prefix = other.prefix;
            this.discrimatorValue = other.discrimatorValue;
            this.metric = other.metric;
            this.unit = other.unit;
            this.tags.putAll(other.tags);
        }

        @Nonnull
        @Override
        public MetricDescriptor withPrefix(String prefix) {
            return new DummyMetricDescriptor(prefix);
        }

        @Nullable
        @Override
        public String prefix() {
            return "";
        }

        @Nonnull
        @Override
        public MetricDescriptor withMetric(String metric) {
            this.metric = metric;
            return this;
        }

        @Override
        public String metric() {
            return metric;
        }

        @Nonnull
        @Override
        public MetricDescriptor withDiscriminator(String discriminatorTag, String discriminatorValue) {
            this.discrimatorValue = discriminatorValue;
            return this;
        }

        @Nullable
        @Override
        public String discriminator() {
            return "";
        }

        @Nullable
        @Override
        public String discriminatorValue() {
            return discrimatorValue;
        }

        @Nonnull
        @Override
        public MetricDescriptor withUnit(ProbeUnit unit) {
            this.unit = unit;
            return this;
        }

        @Nullable
        @Override
        public ProbeUnit unit() {
            return unit;
        }

        @Nonnull
        @Override
        public MetricDescriptor withTag(String tag, String value) {
            tags.put(tag, value);
            return this;
        }

        @Nullable
        @Override
        public String tagValue(String tag) {
            return tags.get(tag);
        }

        @Override
        public void readTags(BiConsumer<String, String> tagReader) {
            tags.forEach(tagReader);
        }

        @Nullable
        @Override
        public String tag(int index) {
            return "";
        }

        @Nullable
        @Override
        public String tagValue(int index) {
            return "";
        }

        @Override
        public int tagCount() {
            return tags.size();
        }

        @Nonnull
        @Override
        public String metricString() {
            return "";
        }

        @Nonnull
        @Override
        public Collection<MetricTarget> excludedTargets() {
            return List.of();
        }

        @Nonnull
        @Override
        public MetricDescriptor withExcludedTarget(MetricTarget target) {
            return null;
        }

        @Nonnull
        @Override
        public MetricDescriptor withExcludedTargets(Collection<MetricTarget> excludedTargets) {
            return null;
        }

        @Nonnull
        @Override
        public MetricDescriptor withIncludedTarget(MetricTarget target) {
            return null;
        }

        @Override
        public boolean isTargetExcluded(MetricTarget target) {
            return false;
        }

        @Override
        public boolean isTargetIncluded(MetricTarget target) {
            return false;
        }

        @Nonnull
        @Override
        public MetricDescriptor copy() {
            return new DummyMetricDescriptor(this);
        }

        @Nonnull
        @Override
        public MetricDescriptor copy(MetricDescriptor descriptor) {
            return null;
        }

        @Nonnull
        @Override
        public MetricDescriptor reset() {
            return null;
        }
    }

    public static final CPGroupId cpGroup1 = new CPGroupId() {
        @Override
        public String getName() {
            return "g1";
        }

        @Override
        public long getId() {
            return 1;
        }
    };

    public static final CPGroupId cpGroup2 = new CPGroupId() {
        @Override
        public String getName() {
            return "g2";
        }

        @Override
        public long getId() {
            return 2;
        }
    };

    public static void checkLiveAndDestroyedCounts(DummyCollector collector,
                                                   CPGroupId cpGroupId,
                                                   int expectedTotalCollectorEntries,
                                                   int expectedLive,
                                                   int expectedDestroyed) {
        assertEquals(expectedTotalCollectorEntries, collector.getEntries().size());
        String cpGroupName = cpGroupId.getName();
        var live = collector.getEntries().stream().filter(r -> r.descriptor().metric().equals("live.count") && r.descriptor().discriminatorValue().equals(cpGroupName)).findFirst().get();
        assertEquals(expectedLive, live.value());
        var destroyed = collector.getEntries().stream().filter(r -> r.descriptor().metric().equals("destroyed.count") && r.descriptor().discriminatorValue().equals(cpGroupName)).findFirst().get();
        assertEquals(expectedDestroyed, destroyed.value());
    }

    @BeforeEach
    void setUp() {
        nodeEngine = mock(NodeEngine.class);
        ClusterService clusterService = mock(ClusterService.class);
        CPSubsystemConfig cpSubsystemConfig = mock(CPSubsystemConfig.class);

        Config config = mock(Config.class);
        CPMapConfig mapConfig = mock(CPMapConfig.class);
        Node node = mock(Node.class);

        when(nodeEngine.getClusterService()).thenReturn(clusterService);
        when(nodeEngine.getConfig()).thenReturn(config);
        when(nodeEngine.getNode()).thenReturn(node);
        when(config.getCPSubsystemConfig()).thenReturn(cpSubsystemConfig);
        when(cpSubsystemConfig.findCPMapConfig(anyString())).thenReturn(mapConfig);
        when(mapConfig.isPurgeEnabled()).thenReturn(true);

        when(clusterService.getClusterVersion()).thenReturn(Versions.CURRENT_CLUSTER_VERSION);
    }

    @Test
    void testSummaryMetrics_Empty() {
        var root = new DummyMetricDescriptor();
        var collector = new DummyCollector();
        CPMapService.addSummaryMetrics(root, collector, new HashMap<>());
        assertEquals(0, collector.getEntries().size());
    }

    @Test
    void testSummaryMetrics_OneCpGroup() {
        CPSubsystemConfig cpSubsystemConfig = mock(CPSubsystemConfig.class);

        var cpGroup1Reg = new CPMapRegistry(cpGroup1);
        cpGroup1Reg.getMapStore(nodeEngine, "m1", cpSubsystemConfig);
        cpGroup1Reg.getMapStore(nodeEngine, "m2", cpSubsystemConfig);
        var root = new DummyMetricDescriptor();
        var collector = new DummyCollector();
        CPMapService.addSummaryMetrics(root, collector, Map.of(cpGroup1, cpGroup1Reg));


        checkLiveAndDestroyedCounts(collector, cpGroup1, 2, 2, 0);

        // remove m1
        cpGroup1Reg.destroyMapStore("m1");
        root = new DummyMetricDescriptor();
        collector = new DummyCollector();
        CPMapService.addSummaryMetrics(root, collector, Map.of(cpGroup1, cpGroup1Reg));
        checkLiveAndDestroyedCounts(collector, cpGroup1, 2, 1, 1);

        // remove m2
        cpGroup1Reg.destroyMapStore("m2");
        root = new DummyMetricDescriptor();
        collector = new DummyCollector();
        CPMapService.addSummaryMetrics(root, collector, Map.of(cpGroup1, cpGroup1Reg));
        checkLiveAndDestroyedCounts(collector, cpGroup1, 2, 0, 2);
    }

    @Test
    void testSummaryMetrics_TwoCpGroup() {
        CPSubsystemConfig cpSubsystemConfig = mock(CPSubsystemConfig.class);

        var cpGroup1Reg = new CPMapRegistry(cpGroup1);
        cpGroup1Reg.getMapStore(nodeEngine, "m1", cpSubsystemConfig);
        cpGroup1Reg.getMapStore(nodeEngine, "m2", cpSubsystemConfig);

        var cpGroup2Reg = new CPMapRegistry(cpGroup2);
        cpGroup2Reg.getMapStore(nodeEngine, "m3", cpSubsystemConfig);
        cpGroup2Reg.getMapStore(nodeEngine, "m4", cpSubsystemConfig);

        var root = new DummyMetricDescriptor();
        var collector = new DummyCollector();
        CPMapService.addSummaryMetrics(root, collector, Map.of(cpGroup1, cpGroup1Reg, cpGroup2, cpGroup2Reg));

        checkLiveAndDestroyedCounts(collector, cpGroup1, 4, 2, 0);
        checkLiveAndDestroyedCounts(collector, cpGroup2, 4, 2, 0);

        // remove m1
        cpGroup1Reg.destroyMapStore("m1");
        root = new DummyMetricDescriptor();
        collector = new DummyCollector();
        CPMapService.addSummaryMetrics(root, collector, Map.of(cpGroup1, cpGroup1Reg, cpGroup2, cpGroup2Reg));
        checkLiveAndDestroyedCounts(collector, cpGroup1, 4, 1, 1);
        checkLiveAndDestroyedCounts(collector, cpGroup2, 4, 2, 0);

        // remove m2
        cpGroup1Reg.destroyMapStore("m2");
        root = new DummyMetricDescriptor();
        collector = new DummyCollector();
        CPMapService.addSummaryMetrics(root, collector, Map.of(cpGroup1, cpGroup1Reg, cpGroup2, cpGroup2Reg));
        checkLiveAndDestroyedCounts(collector, cpGroup1, 4, 0, 2);
        checkLiveAndDestroyedCounts(collector, cpGroup2, 4, 2, 0);

        // remove m3
        cpGroup2Reg.destroyMapStore("m3");
        root = new DummyMetricDescriptor();
        collector = new DummyCollector();
        CPMapService.addSummaryMetrics(root, collector, Map.of(cpGroup1, cpGroup1Reg, cpGroup2, cpGroup2Reg));
        checkLiveAndDestroyedCounts(collector, cpGroup1, 4, 0, 2);
        checkLiveAndDestroyedCounts(collector, cpGroup2, 4, 1, 1);

        // remove m4
        cpGroup2Reg.destroyMapStore("m4");
        root = new DummyMetricDescriptor();
        collector = new DummyCollector();
        CPMapService.addSummaryMetrics(root, collector, Map.of(cpGroup1, cpGroup1Reg, cpGroup2, cpGroup2Reg));
        checkLiveAndDestroyedCounts(collector, cpGroup1, 4, 0, 2);
        checkLiveAndDestroyedCounts(collector, cpGroup2, 4, 0, 2);
    }
}
