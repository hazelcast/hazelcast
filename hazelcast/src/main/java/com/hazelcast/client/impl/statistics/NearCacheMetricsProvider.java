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

package com.hazelcast.client.impl.statistics;

import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ProxyManager;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.monitor.impl.NearCacheStatsImpl;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NEARCACHE_DISCRIMINATOR_NAME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.NEARCACHE_PREFIX;

class NearCacheMetricsProvider implements DynamicMetricsProvider {

    private final ProxyManager proxyManager;

    NearCacheMetricsProvider(ProxyManager proxyManager) {
        this.proxyManager = proxyManager;
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        descriptor.withPrefix(NEARCACHE_PREFIX);

        ClientContext clientContext = proxyManager.getContext();
        if (clientContext == null) {
            return;
        }

        clientContext.getNearCacheManagers().values().stream()
                .flatMap(nearCacheManager -> nearCacheManager.listAllNearCaches().stream())
                .forEach(nearCache -> {
                    String nearCacheName = nearCache.getName();
                    NearCacheStatsImpl nearCacheStats = (NearCacheStatsImpl) nearCache.getNearCacheStats();
                    context.collect(descriptor.copy().withDiscriminator(NEARCACHE_DISCRIMINATOR_NAME, nearCacheName),
                            nearCacheStats);
                });
    }
}
