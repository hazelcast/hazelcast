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
import com.hazelcast.internal.util.ServiceLoader;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.ClusterProperty;

import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.util.phonehome.MetricsProvider.TIMEOUT;
import static java.lang.System.getenv;
import static java.util.concurrent.TimeUnit.DAYS;

/**
 * Pings phone home server with cluster info daily.
 */
@SuppressWarnings("ClassDataAbstractionCoupling")
public class PhoneHome {
    private static final String FALSE = "false";
    private static final String DEFAULT_BASE_PHONE_HOME_URL = "https://phonehome.hazelcast.com/ping";
    private static final String FACTORY_ID = MetricsProvider.class.getName();

    protected final Node node;
    volatile ScheduledFuture<?> phoneHomeFuture;
    private final ILogger logger;
    private final String basePhoneHomeUrl;
    private final List<MetricsProvider> metricsProviders = new ArrayList<>();

    public PhoneHome(Node node) {
        this(node, DEFAULT_BASE_PHONE_HOME_URL);
    }

    // visible for testing
    PhoneHome(Node node, String basePhoneHomeUrl) {
        this.node = node;
        logger = node.getLogger(PhoneHome.class);
        this.basePhoneHomeUrl = basePhoneHomeUrl;
        try {
            ServiceLoader.iterator(MetricsProvider.class, FACTORY_ID, node.getConfigClassLoader())
                    .forEachRemaining(metricsProviders::add);
        } catch (Exception e) {
            sneakyThrow(e);
        }
    }

    /**
     * Schedules a daily phone home metrics collection cycle, upon which the collected metrics
     * are sent to the PhoneHome application. The first cycle is initiated immediately.
     */
    public void start() {
        if (!isPhoneHomeEnabled(node)) {
            return;
        }
        try {
            phoneHomeFuture = node.nodeEngine.getExecutionService()
                    .scheduleWithRepetition("PhoneHome", () -> phoneHome(false), 0, 1, DAYS);
        } catch (RejectedExecutionException e) {
            logger.warning("Could not schedule phone home task! Most probably Hazelcast failed to start.");
        }
    }

    public void shutdown() {
        if (phoneHomeFuture != null) {
            phoneHomeFuture.cancel(true);
        }
    }

    private void postPhoneHomeData(String requestBody) {
        HttpURLConnection conn = null;
        try {
            URL url = URI.create(basePhoneHomeUrl).toURL();
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(TIMEOUT);
            conn.setReadTimeout(TIMEOUT);
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.connect();
            try (OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream(), StandardCharsets.UTF_8)) {
                writer.write(requestBody);
                writer.flush();
            }
            conn.getContent();
        } catch (Exception ignored) {
            // no-op
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
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
        MetricsCollectionContext context = new MetricsCollectionContext();
        collectMetrics(context);
        if (!pretend) {
            postPhoneHomeData(context.getQueryString());
        }
        return context.getParameters();
    }

    public void collectMetrics(MetricsCollectionContext context) {
        for (MetricsProvider metricsProvider : metricsProviders) {
            try {
                metricsProvider.provideMetrics(node, context);
            } catch (Exception e) {
                logger.warning("Some metrics were not recorded ", e);
            }
        }
    }

    public static boolean isPhoneHomeEnabled(Node node) {
        if (!node.getProperties().getBoolean(ClusterProperty.PHONE_HOME_ENABLED)) {
            return false;
        }
        return !FALSE.equals(getenv("HZ_PHONE_HOME_ENABLED"));
    }
}
