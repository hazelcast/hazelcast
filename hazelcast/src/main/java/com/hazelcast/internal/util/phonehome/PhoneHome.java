/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.ClusterProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.lang.System.getenv;

/**
 * Pings phone home server with cluster info daily.
 */
public class PhoneHome {

    private static final String FALSE = "false";
    private static final String DEFAULT_BASE_PHONE_HOME_URL = "http://phonehome.hazelcast.com/ping";
    private static final MetricsCollector CLOUD_INFO_COLLECTOR = new CloudInfoCollector();

    protected final Node hazelcastNode;
    volatile ScheduledFuture<?> phoneHomeFuture;
    private final ILogger logger;
    private final String basePhoneHomeUrl;
    private final List<MetricsCollector> metricsCollectorList;

    public PhoneHome(Node node) {
        this(node, DEFAULT_BASE_PHONE_HOME_URL, CLOUD_INFO_COLLECTOR);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    PhoneHome(Node node, String baseUrl, MetricsCollector... additionalCollectors) {
        hazelcastNode = node;
        logger = hazelcastNode.getLogger(PhoneHome.class);
        basePhoneHomeUrl = baseUrl;
        metricsCollectorList = new ArrayList<>(additionalCollectors.length + 7);
        Collections.addAll(metricsCollectorList,
                new BuildInfoCollector(), new ClusterInfoCollector(), new ClientInfoCollector(),
                new MapInfoCollector(), new OSInfoCollector(), new DistributedObjectCounterCollector(),
                new CacheInfoCollector());
        Collections.addAll(metricsCollectorList, additionalCollectors);
    }

    public void check() {
        if (!hazelcastNode.getProperties().getBoolean(ClusterProperty.PHONE_HOME_ENABLED)) {
            return;
        }
        if (FALSE.equals(getenv("HZ_PHONE_HOME_ENABLED"))) {
            return;
        }
        try {
            phoneHomeFuture = hazelcastNode.nodeEngine.getExecutionService()
                                                      .scheduleWithRepetition("PhoneHome",
                                                              () -> phoneHome(false), 0, 1, TimeUnit.DAYS);
        } catch (RejectedExecutionException e) {
            logger.warning("Could not schedule phone home task! Most probably Hazelcast failed to start.");
        }
    }

    public void shutdown() {
        if (phoneHomeFuture != null) {
            phoneHomeFuture.cancel(true);
        }
    }

    /**
     * Performs a phone request for {@code node} and returns the generated request
     * parameters. If {@code pretend} is {@code true}, only returns the parameters
     * without actually performing the request.
     *
     * @param pretend if {@code true}, do not perform the request
     * @return the generated request parameters
     */
    public Map<String, String> phoneHome(boolean pretend) {
        PhoneHomeParameterCreator parameterCreator = createParameters();
        if (!pretend) {
            String urlStr = basePhoneHomeUrl + parameterCreator.build();
            MetricsCollector.fetchWebService(urlStr);
        }
        return parameterCreator.getParameters();
    }

    public PhoneHomeParameterCreator createParameters() {
        PhoneHomeParameterCreator parameterCreator = new PhoneHomeParameterCreator();
        for (MetricsCollector metricsCollector : metricsCollectorList) {
            try {
                metricsCollector.forEachMetric(hazelcastNode,
                        (type, value) -> parameterCreator.addParam(type.getRequestParameterName(), value));
            } catch (Exception e) {
                logger.warning("Some metrics were not recorded ", e);
            }
        }
        return parameterCreator;
    }
}
