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

import com.hazelcast.client.impl.proxy.RealTimeClientMapProxy;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ProxyManager;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricDescriptorConstants;
import com.hazelcast.internal.metrics.MetricsCollectionContext;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_DISCRIMINATOR_NAME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_PREFIX;

class RealTimeMetricsProvider implements DynamicMetricsProvider {

    private final ProxyManager proxyManager;

    RealTimeMetricsProvider(ProxyManager proxyManager) {
        this.proxyManager = proxyManager;
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        ClientContext clientContext = proxyManager.getContext();
        if (clientContext == null) {
            return;
        }

        descriptor.withMetric(MetricDescriptorConstants.CLIENT_METRIC_LATENCY);
        clientContext.getProxyManager().getDistributedObjects().stream()
                .filter(proxy -> proxy instanceof RealTimeClientMapProxy)
                        .forEach(proxy -> {
                            RealTimeClientMapProxy realTimeClientMapProxy = (RealTimeClientMapProxy) proxy;
                            context.collect(descriptor
                                            .withDiscriminator(MAP_DISCRIMINATOR_NAME, realTimeClientMapProxy.getName())
                                            .withTag(OPERATION_PREFIX, RealTimeClientMapProxy.PUT_OPERATION_NAME)
                                            .withTag(RealTimeClientMapProxy.LIMIT_NAME, realTimeClientMapProxy.getLimitString()),
                                    realTimeClientMapProxy.getPutLatency());
                            descriptor.reset();
                            context.collect(descriptor
                                            .withMetric(MetricDescriptorConstants.CLIENT_METRIC_LATENCY)
                                            .withDiscriminator(MAP_DISCRIMINATOR_NAME, realTimeClientMapProxy.getName())
                                            .withTag(OPERATION_PREFIX, RealTimeClientMapProxy.GET_OPERATION_NAME)
                                            .withTag(RealTimeClientMapProxy.LIMIT_NAME, realTimeClientMapProxy.getLimitString()),
                                    realTimeClientMapProxy.getGetLatency());
                        });
    }
}
