package com.hazelcast.internal.probing;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;
import static com.hazelcast.nio.IOUtil.closeResource;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.util.concurrent.atomic.AtomicBoolean;

import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.probing.ProbeRegistry.ProbeRenderContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.Clock;

public final class ProbeDataSender implements Runnable, Closeable {

    interface HttpURLConnectionProvider {

        HttpURLConnection openConnection();
    }

    private static final int CONNECTION_TIMEOUT_MILLIS = 3000;

    private static final ILogger LOGGER = Logger.getLogger(ProbeDataSender.class);

    private final ProbeRenderContext probes;
    private final long intervalMs;
    private final HttpURLConnectionProvider connectionProvider;
    private final String timeKey;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ProbeDataSender(ProbeRenderContext probes, long intervalMs, HttpURLConnectionProvider connectionProvider, String group, String member) {
        this.probes = probes;
        this.intervalMs = intervalMs;
        this.connectionProvider = connectionProvider;
        this.timeKey = "group="+group+" member="+member+" name=time";
    }

    @Override
    public void close() {
        running.compareAndSet(false, true);
    }

    @Override
    public void run() {
        running.set(true);
        try {
            while (isRunning()) {
                long startMs = Clock.currentTimeMillis();
                sendProbeData();
                long endMs = Clock.currentTimeMillis();
                long sleepMs = intervalMs - (endMs - startMs);
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

    public boolean isRunning() {
        return running.get();
    }

    private void sendProbeData() throws IOException {
        OutputStream out = null;
        try {
            HttpURLConnection conn = openConnection();
            out = conn.getOutputStream();
            CompressingProbeRenderer renderer = new CompressingProbeRenderer(out);
            // start with the common group member and time
            renderer.render(timeKey, Clock.currentTimeMillis()); 
            probes.renderAt(ProbeLevel.INFO, renderer);
            renderer.done();
            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                LOGGER.warning("Failed to send probe data to " + conn.getURL() + ". Status code: "
                        + conn.getResponseCode());
            }
        } finally {
            closeResource(out);
        }
    }

    private HttpURLConnection openConnection() throws IOException {
        HttpURLConnection conn = connectionProvider.openConnection();
        conn.setDoOutput(true);
        conn.setConnectTimeout(CONNECTION_TIMEOUT_MILLIS);
        conn.setReadTimeout(CONNECTION_TIMEOUT_MILLIS);
        conn.setRequestProperty("Accept", "application/octet-stream");
        conn.setRequestProperty("Content-Type", "application/octet-stream");
        conn.setRequestMethod("POST");
        return conn;
    }

}
