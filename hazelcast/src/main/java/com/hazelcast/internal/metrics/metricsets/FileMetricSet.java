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

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsRegistry;

import java.io.File;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.FILE_DISCRIMINATOR_DIR;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.FILE_DISCRIMINATOR_VALUE_DIR;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.FILE_METRIC_FREESPACE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.FILE_METRIC_TOTALSPACE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.FILE_METRIC_USABLESPACE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.FILE_PREFIX;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.metrics.ProbeUnit.BYTES;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * A MetricSet for files. Currently only displays space statistics for the partition of the user.home
 */
public final class FileMetricSet {

    private FileMetricSet() {
    }

    /**
     * Registers all the metrics in this metric pack.
     *
     * @param metricsRegistry the MetricsRegistry upon which the metrics are registered.
     */
    public static void register(MetricsRegistry metricsRegistry) {
        checkNotNull(metricsRegistry, "metricsRegistry");

        File file = new File(System.getProperty("user.home"));
        MetricDescriptor descriptor = metricsRegistry
                .newMetricDescriptor()
                .withPrefix(FILE_PREFIX)
                .withDiscriminator(FILE_DISCRIMINATOR_DIR, FILE_DISCRIMINATOR_VALUE_DIR);
        metricsRegistry.registerStaticProbe(file, descriptor, FILE_METRIC_FREESPACE, MANDATORY, BYTES, File::getFreeSpace);
        metricsRegistry.registerStaticProbe(file, descriptor, FILE_METRIC_TOTALSPACE, MANDATORY, BYTES, File::getTotalSpace);
        metricsRegistry.registerStaticProbe(file, descriptor, FILE_METRIC_USABLESPACE, MANDATORY, BYTES, File::getUsableSpace);
    }
}
