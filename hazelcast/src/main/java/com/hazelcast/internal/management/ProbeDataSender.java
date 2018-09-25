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

import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeRenderContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.Clock;

/**
 * A {@link Runnable} sending probe data of {@link ProbeRenderContext} to given
 * the {@link URL} using a {@link CompressingProbeRenderer}.
 */
final class ProbeDataSender implements Runnable, Closeable {

    private static final int CONNECTION_TIMEOUT_MILLIS = 3000;
    private static final int STANDED_INTERVAL_MILLIS = 1000;

    private static final ILogger LOGGER = Logger.getLogger(ProbeDataSender.class);

    private volatile URL url;
    private final ManagementCenterConnectionFactory connectionFactory;

    private final ProbeRenderContext renderContext;
    private final long intervalMs;
    private final String timeKey;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ProbeDataSender(URL url, ManagementCenterConnectionFactory connectionFactory,
            String group, String member, ProbeRenderContext renderContext) {
        this(url, connectionFactory, group, member, renderContext, STANDED_INTERVAL_MILLIS);
    }

    public ProbeDataSender(URL url, ManagementCenterConnectionFactory connectionFactory,
            String group, String member, ProbeRenderContext renderContext, long intervalMs) {
        this.url = url;
        this.connectionFactory = connectionFactory;
        this.renderContext = renderContext;
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
            boolean warnSlow = true;
            boolean warnFailure = true;
            long currentIntervalMs = intervalMs;
            while (isRunning()) {
                long startMs = Clock.currentTimeMillis();
                URL currentURL = url;
                boolean isFailure = !sendProbeData(currentURL);
                long endMs = Clock.currentTimeMillis();
                long durationMs = endMs - startMs;
                boolean isSlow = durationMs > intervalMs / 2;
                if (isFailure && warnFailure) {
                    LOGGER.warning("Failed to send probe data to " + currentURL);
                }
                if (!isFailure && isSlow && warnSlow) {
                    LOGGER.warning("Sending probe data took " + durationMs + " ms");
                }
                warnFailure = !isFailure;
                warnSlow = !isSlow;
                currentIntervalMs = isFailure ? 5 * intervalMs : intervalMs;
                long sleepMs = currentIntervalMs - durationMs;
                if (sleepMs > 0) {
                    Thread.sleep(sleepMs);
                }
            }
        } catch (Throwable t) {
            inspectOutOfMemoryError(t);
            if (!(t instanceof InterruptedException)) {
                LOGGER.warning("Exception occurred while sending probe data", t);
            }
        }
    }

    private boolean sendProbeData(URL url) throws IOException {
        if (url == null) {
            return false;
        }
        OutputStream out = null;
        try {
            HttpURLConnection conn = openConnection(url);
            out = conn.getOutputStream();
            CompressingProbeRenderer renderer = new CompressingProbeRenderer(out);
            // start with the common group member and time
            renderer.render(timeKey, Clock.currentTimeMillis());
            renderContext.render(ProbeLevel.INFO, renderer);
            renderer.done();
            return conn.getResponseCode() != HTTP_OK;
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
