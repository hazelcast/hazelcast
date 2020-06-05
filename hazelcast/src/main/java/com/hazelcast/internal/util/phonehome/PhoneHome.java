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
package com.hazelcast.internal.util.phonehome;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.ClusterProperty;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static java.lang.System.getenv;

/**
 * Pings phone home server with cluster info daily.
 */
public class PhoneHome {

    private static final int TIMEOUT = 1000;

    private static final String FALSE = "false";
    private static final String DEFAULT_BASE_PHONE_HOME_URL = "http://phonehome.hazelcast.com/ping";

    volatile ScheduledFuture<?> phoneHomeFuture;
    private final ILogger logger;
    private final String basePhoneHomeUrl;

    private final Node hazelcastNode;
    private final List<MetricsCollector> metricsCollectorList = Arrays.asList(new BuildInfoCollector(),
            new ClusterInfoCollector(), new ClientInfoCollector(), new MapInfoCollector(), new OSInfoCollector());

    public PhoneHome(Node node) {
        this(node, DEFAULT_BASE_PHONE_HOME_URL);
    }

    PhoneHome(Node node, String baseurl) {
        hazelcastNode = node;
        logger = hazelcastNode.getLogger(com.hazelcast.internal.util.phonehome.PhoneHome.class);
        basePhoneHomeUrl = baseurl;
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
            fetchWebService(urlStr);
        }

        return parameterCreator.getParameters();
    }

    public PhoneHomeParameterCreator createParameters() {

        PhoneHomeParameterCreator parameterCreator = new PhoneHomeParameterCreator();

        metricsCollectorList.forEach((metricsCollector -> parameterCreator.
                addMap(metricsCollector.computeMetrics(hazelcastNode))));

        return parameterCreator;
    }


    private void fetchWebService(String urlStr) {
        InputStream in = null;
        try {
            URL url = new URL(urlStr);
            URLConnection conn = url.openConnection();
            conn.setRequestProperty("User-Agent", "Mozilla/5.0");
            conn.setConnectTimeout(TIMEOUT * 2);
            conn.setReadTimeout(TIMEOUT * 2);
            in = new BufferedInputStream(conn.getInputStream());
        } catch (Exception ignored) {
            ignore(ignored);
        } finally {
            closeResource(in);
        }
    }

}
