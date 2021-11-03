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
        metricsConsumer.accept(PhoneHomeMetrics.REST_MAP_GET_SUCCESS, collector.getMapGetSuccessCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_MAP_GET_FAILURE, collector.getMapGetFailureCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_ACCESSED_MAP_COUNT, collector.getAccessedMapCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_QUEUE_POST_SUCCESS, collector.getQueuePostSuccessCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_QUEUE_POST_FAILURE, collector.getQueuePostFailureCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_ACCESSED_QUEUE_COUNT, collector.getAccessedQueueCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_REQUEST_COUNT, collector.getRequestCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_UNIQUE_REQUEST_COUNT, collector.getUniqueRequestCount());
    }
}
