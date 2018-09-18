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

package com.hazelcast.jet.impl.metrics.mancenter;

import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.jet.impl.metrics.JetMetricsService;
import com.hazelcast.jet.impl.metrics.mancenter.ConcurrentArrayRingbuffer.RingbufferSlice;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.WaitNotifyKey;

import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

public class ReadMetricsOperation extends Operation implements BlockingOperation, ReadonlyOperation {

    private static final int MIN_TIMEOUT_SECONDS = 5;
    private long offset;
    private RingbufferSlice<Entry<Long, byte[]>> resultSet;

    public ReadMetricsOperation(long offset, int collectionIntervalSeconds) {
        this.offset = offset;
        int timeoutSeconds = Math.min(MIN_TIMEOUT_SECONDS, collectionIntervalSeconds * 2);
        setWaitTimeout(TimeUnit.SECONDS.toMillis(timeoutSeconds));
    }

    @Override
    public Object getResponse() {
        return resultSet;
    }

    @Override
    public String getServiceName() {
        return JetMetricsService.SERVICE_NAME;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        JetMetricsService service = getService();
        return service.waitNotifyKey();
    }

    @Override
    public boolean shouldWait() {
        JetMetricsService service = getService();
        this.resultSet = service.readMetrics(offset);
        return resultSet.elements().isEmpty();
    }

    @Override
    public void onWaitExpire() {
        sendResponse(new OperationTimeoutException("No metrics were retrieved for " + getWaitTimeout() + "ms"));
    }

}
