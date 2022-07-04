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

package com.hazelcast.internal.util.phonehome;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.ascii.rest.RestCallCollector;

import java.util.function.BiConsumer;

public class RestApiMetricsCollector implements MetricsCollector {

    @Override
    public void forEachMetric(Node node, BiConsumer<PhoneHomeMetrics, String> metricsConsumer) {
        RestCallCollector collector = node.getTextCommandService().getRestCallCollector();
        String enabled = node.getConfig().getNetworkConfig().getRestApiConfig().isEnabledAndNotEmpty() ? "1" : "0";
        metricsConsumer.accept(PhoneHomeMetrics.REST_ENABLED, enabled);
        metricsConsumer.accept(PhoneHomeMetrics.REST_MAP_POST_SUCCESS, collector.getMapPostSuccessCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_MAP_POST_FAILURE, collector.getMapPostFailureCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_MAP_DELETE_SUCCESS, collector.getMapDeleteSuccessCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_MAP_DELETE_FAILURE, collector.getMapDeleteFailureCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_MAP_GET_SUCCESS, collector.getMapGetSuccessCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_MAP_GET_FAILURE, collector.getMapGetFailureCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_MAP_TOTAL_REQUEST_COUNT, collector.getTotalMapRequestCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_ACCESSED_MAP_COUNT, collector.getAccessedMapCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_QUEUE_POST_SUCCESS, collector.getQueuePostSuccessCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_QUEUE_POST_FAILURE, collector.getQueuePostFailureCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_QUEUE_GET_SUCCESS, collector.getQueueGetSuccessCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_QUEUE_GET_FAILURE, collector.getQueueGetFailureCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_QUEUE_DELETE_SUCCESS, collector.getQueueDeleteSuccessCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_QUEUE_DELETE_FAILURE, collector.getQueueDeleteFailureCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_ACCESSED_QUEUE_COUNT, collector.getAccessedQueueCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_QUEUE_TOTAL_REQUEST_COUNT, collector.getTotalQueueRequestCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_CONFIG_UPDATE_SUCCESS, collector.getConfigUpdateSuccessCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_CONFIG_UPDATE_FAILURE, collector.getConfigUpdateFailureCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_CONFIG_RELOAD_SUCCESS, collector.getConfigReloadSuccessCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_CONFIG_RELOAD_FAILURE, collector.getConfigReloadFailureCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_REQUEST_COUNT, collector.getRequestCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_UNIQUE_REQUEST_COUNT, collector.getUniqueRequestCount());
    }
}
