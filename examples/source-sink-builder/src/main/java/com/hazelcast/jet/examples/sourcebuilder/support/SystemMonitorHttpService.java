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

package com.hazelcast.jet.examples.sourcebuilder.support;

import io.undertow.Undertow;
import io.undertow.server.HttpServerExchange;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static io.undertow.util.Headers.CONTENT_TYPE;
import static java.lang.Runtime.getRuntime;

/**
 * Starts a thread that records a time series of used JVM heap memory.
 * Starts an HTTP server that delivers these results. The HTTP response
 * consists of one timestamped measurement per line. The server delivers
 * the data accumulated since the last request and then forgets it.
 */
public class SystemMonitorHttpService {
    private final Runtime runtime = getRuntime();
    private final BlockingQueue<MemoryUsageMetric> queue = new LinkedBlockingQueue<>();

    {
        Thread t = new Thread(() -> {
            while (true) {
                long monitoredValue = runtime.totalMemory() - runtime.freeMemory();
                queue.add(new MemoryUsageMetric(System.currentTimeMillis(), monitoredValue));
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        t.setName("mock-system-monitor");
        t.setDaemon(true);
        t.start();
    }

    public Undertow httpServer() {
        return Undertow.builder()
                       .addHttpListener(8008, "localhost")
                       .setHandler(this::handleRequest)
                       .build();
    }

    private void handleRequest(HttpServerExchange exchange) {
        exchange.getResponseHeaders().put(CONTENT_TYPE, "text/plain");
        List<MemoryUsageMetric> tmpList = new ArrayList<>();
        queue.drainTo(tmpList);
        if (tmpList.isEmpty()) {
            return;
        }
        StringBuilder b = new StringBuilder();
        for (MemoryUsageMetric reading : tmpList) {
            b.append(reading.timestamp()).append(' ')
             .append(reading.memoryUsage()).append('\n');
        }
        exchange.getResponseSender().send(b.toString());
    }
}
