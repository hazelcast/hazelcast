package com.hazelcast.profiler;

import com.hazelcast.internal.util.tracing.TracingContext;
import com.hazelcast.jet.impl.util.IOUtil;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class AsyncProfilerTracingContext implements TracingContext {
    private static final String FILE_LIBASYNC_PROFILER_SO = "/tmp/libasyncProfiler.so";
    private static final String CP_LIBASYNC_PROFILER_SO = "/libasyncProfiler.so";
    private static final String EVENT = "wall";
    private final AsyncProfiler asyncProfiler;
    private final ThreadLocal<String> ecid = new ThreadLocal<>();

    public AsyncProfilerTracingContext() throws IOException {
        try (InputStream inputStream = this.getClass().getResourceAsStream(CP_LIBASYNC_PROFILER_SO);
             OutputStream outputStream = new FileOutputStream(FILE_LIBASYNC_PROFILER_SO)
        ) {
            IOUtil.copyStream(inputStream, outputStream);
        }
        asyncProfiler = AsyncProfiler.getInstance(FILE_LIBASYNC_PROFILER_SO);
        asyncProfiler.execute("start,jfr,event=" + EVENT + ",file=prof.jfr");
    }

    @Override
    public void setCorrelationId(String correlationId) {
        if (correlationId == null) {
            asyncProfiler.clearEcid();
            ecid.remove();
        } else {
            asyncProfiler.setEcid(correlationId);
            ecid.set(correlationId);
        }
    }

    @Override
    public void clearCorrelationId() {
        asyncProfiler.clearEcid();
        ecid.remove();
    }

    @Override
    public String getCorrelationId() {
        return ecid.get();
    }

    @Override
    public void close() throws Exception {
        asyncProfiler.execute("stop,jfr,event=" + EVENT + ",file=prof.jfr");
    }
}
