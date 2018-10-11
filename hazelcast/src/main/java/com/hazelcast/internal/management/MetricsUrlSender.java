/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;
import static com.hazelcast.nio.IOUtil.closeResource;
import static java.net.HttpURLConnection.HTTP_OK;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.atomic.AtomicBoolean;

import com.hazelcast.internal.metrics.CollectionContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.Clock;

/**
 * A {@link Runnable} sending probe data of {@link CollectionContext} to given
 * the {@link URL} using a {@link MetricsStreamer}.
 */
final class MetricsUrlSender implements Runnable, Closeable {

    private static final int RECONNECT_SLOWDOWN_FACTOR = 5;
    private static final int CONNECTION_TIMEOUT_MILLIS = 3000;
    private static final int STANDED_INTERVAL_MILLIS = 1000;

    private static final ILogger LOGGER = Logger.getLogger(MetricsUrlSender.class);

    private volatile URL url;
    private final ManagementCenterConnectionFactory connectionFactory;

    private final CollectionContext context;
    private final long intervalMs;
    private final String timeKey;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final String member;

    public MetricsUrlSender(URL url, ManagementCenterConnectionFactory connectionFactory,
            String group, String member, CollectionContext context) {
        this(url, connectionFactory, group, member, context, STANDED_INTERVAL_MILLIS);
    }

    public MetricsUrlSender(URL url, ManagementCenterConnectionFactory connectionFactory,
            String group, String member, CollectionContext context, long intervalMs) {
        this.url = url;
        this.connectionFactory = connectionFactory;
        this.member = member;
        this.context = context;
        this.intervalMs = intervalMs;
        this.timeKey = "group=" + group + " member=" + member + " time";
    }

    @Override
    public void close() {
        running.compareAndSet(false, true);
    }

    public boolean isRunning() {
        return running.get();
    }

    public void setUrl(URL url) {
        this.url = url;
    }

    @Override
    public void run() {
        running.set(true);
        try {
            boolean assumeFast = true;
            boolean assumeConnected = true;
            long currentIntervalMs = intervalMs;
            while (isRunning()) {
                long startMs = Clock.currentTimeMillis();
                URL currentURL = url;
                boolean isFailure = !collectAndSend(currentURL, assumeConnected);
                long endMs = Clock.currentTimeMillis();
                long durationMs = endMs - startMs;
                boolean isSlow = durationMs > intervalMs / 2;
                logConnectionProblems(currentURL, durationMs, assumeFast, isSlow, assumeConnected,
                        isFailure);
                assumeConnected = !isFailure;
                assumeFast = !isSlow;
                currentIntervalMs = isFailure ? RECONNECT_SLOWDOWN_FACTOR * intervalMs : intervalMs;
                long sleepMs = currentIntervalMs - durationMs;
                if (sleepMs > 0) {
                    Thread.sleep(sleepMs);
                }
            }
        } catch (Throwable t) {
            inspectOutOfMemoryError(t);
            if (!(t instanceof InterruptedException)) {
                LOGGER.warning("Unexpected exception in MC probe data sender: ", t);
            }
        }
    }

    private void logConnectionProblems(URL url, long durationMs,
            boolean assumeFast, boolean isSlow, boolean assumeConnected, boolean isFailure) {
        if (isFailure && assumeConnected) {
            LOGGER.info("MC connection to `" + url + "` lost by " + member);
        }
        if (!assumeConnected && !isFailure) {
            LOGGER.info("MC connection to `" + url + "` reestablished by " + member);
        }
        if (!isFailure && isSlow && assumeFast) {
            LOGGER.info("MC probe data transfer is slow for " + member + ": " + durationMs + " ms");
        }
    }

    private boolean collectAndSend(URL url, boolean connected) throws IOException {
        if (url == null) {
            return false;
        }
        OutputStream out = null;
        try {
            HttpURLConnection conn = openConnection(url);
            out = conn.getOutputStream();
            MetricsStreamer streamer = new MetricsStreamer(out);
            // start with the common group member and time
            streamer.collect(timeKey, Clock.currentTimeMillis());
            if (connected) {
                context.collectAll(streamer);
            }
            streamer.done();
            return conn.getResponseCode() == HTTP_OK;
        } catch (Exception e) {
            return false;
        } finally {
            closeResource(out);
        }
    }

    private HttpURLConnection openConnection(URL url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) (connectionFactory != null
                ? connectionFactory.openConnection(url)
                        : url.openConnection());
        conn.setDoOutput(true);
        conn.setConnectTimeout(CONNECTION_TIMEOUT_MILLIS);
        conn.setReadTimeout(CONNECTION_TIMEOUT_MILLIS);
        conn.setRequestProperty("Accept", "application/octet-stream");
        conn.setRequestProperty("Content-Type", "application/octet-stream");
        conn.setRequestMethod("POST");
        return conn;
    }

}
