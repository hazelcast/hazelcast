/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.metrics.management;

import com.hazelcast.jet.impl.metrics.JetMetricsService;
import com.hazelcast.jet.impl.metrics.management.ConcurrentArrayRingbuffer.RingbufferSlice;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ReadonlyOperation;

import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;

public class ReadMetricsOperation extends Operation implements ReadonlyOperation {

    private long offset;

    public ReadMetricsOperation(long offset) {
        this.offset = offset;
    }

    @Override
    public void beforeRun() {
        JetMetricsService service = getService();
        service.getLiveOperationRegistry().register(this);
    }

    @Override
    public void run() {
        ILogger logger = getNodeEngine().getLogger(getClass());
        JetMetricsService service = getService();
        CompletableFuture<RingbufferSlice<Entry<Long, byte[]>>> future = service.readMetrics(offset);
        future.whenComplete(withTryCatch(logger, (slice, error) -> doSendResponse(error != null ? peel(error) : slice)));
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public Object getResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServiceName() {
        return JetMetricsService.SERVICE_NAME;
    }

    private void doSendResponse(Object value) {
        try {
            sendResponse(value);
        } finally {
            final JetMetricsService service = getService();
            service.getLiveOperationRegistry().deregister(this);
        }
    }
}
