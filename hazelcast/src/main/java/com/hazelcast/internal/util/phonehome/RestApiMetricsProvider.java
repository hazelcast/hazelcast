/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_ACCESSED_MAP_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_ACCESSED_QUEUE_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_CONFIG_RELOAD_FAILURE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_CONFIG_RELOAD_SUCCESS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_CONFIG_UPDATE_FAILURE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_CONFIG_UPDATE_SUCCESS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_ENABLED;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_MAP_DELETE_FAILURE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_MAP_DELETE_SUCCESS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_MAP_GET_FAILURE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_MAP_GET_SUCCESS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_MAP_POST_FAILURE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_MAP_POST_SUCCESS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_MAP_TOTAL_REQUEST_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_QUEUE_DELETE_FAILURE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_QUEUE_DELETE_SUCCESS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_QUEUE_GET_FAILURE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_QUEUE_GET_SUCCESS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_QUEUE_POST_FAILURE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_QUEUE_POST_SUCCESS;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_QUEUE_TOTAL_REQUEST_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_REQUEST_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.REST_UNIQUE_REQUEST_COUNT;

class RestApiMetricsProvider implements MetricsProvider {

    @Override
    public void provideMetrics(Node node, MetricsCollectionContext context) {
        RestCallCollector collector = node.getTextCommandService().getRestCallCollector();
        String enabled = node.getConfig().getNetworkConfig().getRestApiConfig().isEnabledAndNotEmpty() ? "1" : "0";
        context.collect(REST_ENABLED, enabled);
        context.collect(REST_MAP_POST_SUCCESS, collector.getMapPostSuccessCount());
        context.collect(REST_MAP_POST_FAILURE, collector.getMapPostFailureCount());
        context.collect(REST_MAP_DELETE_SUCCESS, collector.getMapDeleteSuccessCount());
        context.collect(REST_MAP_DELETE_FAILURE, collector.getMapDeleteFailureCount());
        context.collect(REST_MAP_GET_SUCCESS, collector.getMapGetSuccessCount());
        context.collect(REST_MAP_GET_FAILURE, collector.getMapGetFailureCount());
        context.collect(REST_MAP_TOTAL_REQUEST_COUNT, collector.getTotalMapRequestCount());
        context.collect(REST_ACCESSED_MAP_COUNT, collector.getAccessedMapCount());
        context.collect(REST_QUEUE_POST_SUCCESS, collector.getQueuePostSuccessCount());
        context.collect(REST_QUEUE_POST_FAILURE, collector.getQueuePostFailureCount());
        context.collect(REST_QUEUE_GET_SUCCESS, collector.getQueueGetSuccessCount());
        context.collect(REST_QUEUE_GET_FAILURE, collector.getQueueGetFailureCount());
        context.collect(REST_QUEUE_DELETE_SUCCESS, collector.getQueueDeleteSuccessCount());
        context.collect(REST_QUEUE_DELETE_FAILURE, collector.getQueueDeleteFailureCount());
        context.collect(REST_ACCESSED_QUEUE_COUNT, collector.getAccessedQueueCount());
        context.collect(REST_QUEUE_TOTAL_REQUEST_COUNT, collector.getTotalQueueRequestCount());
        context.collect(REST_CONFIG_UPDATE_SUCCESS, collector.getConfigUpdateSuccessCount());
        context.collect(REST_CONFIG_UPDATE_FAILURE, collector.getConfigUpdateFailureCount());
        context.collect(REST_CONFIG_RELOAD_SUCCESS, collector.getConfigReloadSuccessCount());
        context.collect(REST_CONFIG_RELOAD_FAILURE, collector.getConfigReloadFailureCount());
        context.collect(REST_REQUEST_COUNT, collector.getRequestCount());
        context.collect(REST_UNIQUE_REQUEST_COUNT, collector.getUniqueRequestCount());
    }
}
