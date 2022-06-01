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

package com.hazelcast.internal.metrics.metricsets;

import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLASSLOADING_JET_EXTENSIONS_PREFIX;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLASSLOADING_FULL_METRIC_LOADED_CLASSES_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLASSLOADING_FULL_METRIC_TOTAL_LOADED_CLASSES_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLASSLOADING_FULL_METRIC_UNLOADED_CLASSES_COUNT;
import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.metrics.metricsets.ClassLoadingMetricSet.JET_EXTENSION_TO_CLASS_MAPPING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClassLoadingMetricSetTest extends HazelcastTestSupport {

    private static final String JET_EXTENSION_AVAILABLE_IN_CLASSPATH = "avro";

    private static final ClassLoadingMXBean BEAN = ManagementFactory.getClassLoadingMXBean();

    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
        ClassLoadingMetricSet.register(metricsRegistry);
    }

    @After
    public void tearDown() {
        metricsRegistry.shutdown();
    }

    @Test
    public void utilityConstructor() {
        assertUtilityConstructor(ClassLoadingMetricSet.class);
    }

    @Test
    public void loadedClassesCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge(CLASSLOADING_FULL_METRIC_LOADED_CLASSES_COUNT);

        assertTrueEventually(() -> assertEquals(BEAN.getLoadedClassCount(), gauge.read(), 100));
    }

    @Test
    public void totalLoadedClassesCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge(CLASSLOADING_FULL_METRIC_TOTAL_LOADED_CLASSES_COUNT);

        assertTrueEventually(() -> assertEquals(BEAN.getTotalLoadedClassCount(), gauge.read(), 100));
    }

    @Test
    public void unloadedClassCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge(CLASSLOADING_FULL_METRIC_UNLOADED_CLASSES_COUNT);

        assertTrueEventually(() -> assertEquals(BEAN.getUnloadedClassCount(), gauge.read(), 100));
    }

    @Test
    public void availableJetExtensions() {
        final String metricName = CLASSLOADING_JET_EXTENSIONS_PREFIX + "." + JET_EXTENSION_AVAILABLE_IN_CLASSPATH;
        final LongGauge gauge = metricsRegistry.newLongGauge(metricName);

        assertTrueEventually(() -> assertThat(gauge.read()).isOne());
    }

    @Test
    public void unavailableJetExtensions() {
        JET_EXTENSION_TO_CLASS_MAPPING.keySet().stream()
                .filter(name -> !JET_EXTENSION_AVAILABLE_IN_CLASSPATH.equals(name))
                .forEach(extensionName -> {
                    final String metricName = CLASSLOADING_JET_EXTENSIONS_PREFIX + "." + extensionName;
                    final LongGauge gauge = metricsRegistry.newLongGauge(metricName);

                    assertTrueEventually(() -> assertThat(gauge.read()).isZero());
                });
    }

}
