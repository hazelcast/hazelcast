/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLUSTER_DISCRIMINATOR_ENDPOINT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLUSTER_PREFIX_CONNECTION;

class ClusterConnectionMetricsProvider implements DynamicMetricsProvider {
    private final ClientConnectionManager clientConnectionManager;

    ClusterConnectionMetricsProvider(ClientConnectionManager clientConnectionManager) {
        this.clientConnectionManager = clientConnectionManager;
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        for (ClientConnection connection : clientConnectionManager.getActiveConnections()) {
            context.collect(descriptor
                            .withPrefix(CLUSTER_PREFIX_CONNECTION)
                            .withDiscriminator(CLUSTER_DISCRIMINATOR_ENDPOINT, connection.getRemoteAddress().toString()),
                    connection);
        }
    }
}
