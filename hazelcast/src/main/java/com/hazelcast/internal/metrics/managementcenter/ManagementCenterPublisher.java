/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics.managementcenter;

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.MetricsPublisher;
import com.hazelcast.internal.metrics.impl.LongWordException;
import com.hazelcast.internal.metrics.impl.MetricsCompressor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.ObjLongConsumer;

/**
 * Renderer to serialize metrics to byte[] to be read by Management Center.
 * Additionally, it converts legacy metric names to {@code [metric=<oldName>]}.
 */
public class ManagementCenterPublisher implements MetricsPublisher {

    private final ILogger logger;
    private final ObjLongConsumer<byte[]> consumer;
    private final MetricsCompressor compressor;
    private final AtomicBoolean longWordLogged = new AtomicBoolean();

    public ManagementCenterPublisher(@Nonnull LoggingService loggingService, @Nonnull ObjLongConsumer<byte[]> writeFn) {
        this.consumer = writeFn;
        this.logger = loggingService.getLogger(getClass());
        this.compressor = new MetricsCompressor();
    }

    @Override
    public String name() {
        return "Management Center Publisher";
    }

    @Override
    public void publishLong(MetricDescriptor descriptor, long value) {
        try {
            if (descriptor.isTargetIncluded(MetricTarget.MANAGEMENT_CENTER)) {
                compressor.addLong(descriptor, value);
            }
        } catch (LongWordException e) {
            logLongWordException(e);
        }
    }

    @Override
    public void publishDouble(MetricDescriptor descriptor, double value) {
        try {
            if (descriptor.isTargetIncluded(MetricTarget.MANAGEMENT_CENTER)) {
                compressor.addDouble(descriptor, value);
            }
        } catch (LongWordException e) {
            logLongWordException(e);
        }
    }

    private void logLongWordException(LongWordException e) {
        if (longWordLogged.compareAndSet(false, true)) {
            logger.warning(e.getMessage());
        } else {
            logger.fine(e.getMessage());
        }
    }

    @Override
    public void whenComplete() {
        int count = compressor.count();
        byte[] blob = compressor.getBlobAndReset();

        consumer.accept(blob, System.currentTimeMillis());
        logger.finest(String.format("Collected %,d metrics, %,d bytes", count, blob.length));
    }

    public int getCount() {
        return compressor.count();
    }
}
