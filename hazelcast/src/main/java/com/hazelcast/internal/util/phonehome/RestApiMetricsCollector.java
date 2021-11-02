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
        metricsConsumer.accept(PhoneHomeMetrics.REST_MAP_POST_SUCCESS, collector.getMapPutSuccessCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_MAP_POST_FAILURE, collector.getMapPutFailureCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_MAP_GET_SUCCESS, collector.getMapGetSuccessCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_MAP_GET_FAILURE, collector.getMapGetFailureCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_REQUEST_COUNT, collector.getRequestCount());
        metricsConsumer.accept(PhoneHomeMetrics.REST_UNIQUE_REQUEST_COUNT, collector.getUniqueRequestCount());
    }
}
