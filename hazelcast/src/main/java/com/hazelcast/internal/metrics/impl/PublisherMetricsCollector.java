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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsPublisher;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * {@link MetricsCollector} implementation delegating to the configured
 * publishers.
 */
public class PublisherMetricsCollector implements MetricsCollector {
    private final ILogger logger = Logger.getLogger(PublisherMetricsCollector.class);

    private final MetricsPublisher[] publishers;

    public PublisherMetricsCollector(MetricsPublisher... publishers) {
        this.publishers = publishers;
    }

    public void publishCollectedMetrics() {
        for (int i = 0; i < publishers.length; i++) {
            try {
                publishers[i].whenComplete();
            } catch (OutOfMemoryError e) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
            } catch (Throwable throwable) {
                logger.severe("Error completing publication for publisher "
                        + publishers[i].name(), throwable);
            }
        }
    }

    public void shutdown() {
        for (int i = 0; i < publishers.length; i++) {
            try {
                publishers[i].shutdown();
            } catch (OutOfMemoryError e) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
            } catch (Throwable throwable) {
                logger.severe("Error shutting down metrics publisher "
                        + publishers[i].name(), throwable);
            }
        }
    }

    @Override
    public void collectLong(MetricDescriptor descriptor, long value) {
        for (int i = 0; i < publishers.length; i++) {
            try {
                publishers[i].publishLong(descriptor, value);
            } catch (OutOfMemoryError e) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
            } catch (Throwable throwable) {
                logError(descriptor, value, publishers[i], throwable);
            }
        }
    }

    @Override
    public void collectDouble(MetricDescriptor descriptor, double value) {
        for (int i = 0; i < publishers.length; i++) {
            try {
                publishers[i].publishDouble(descriptor, value);
            } catch (OutOfMemoryError e) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
            } catch (Throwable throwable) {
                logError(descriptor, value, publishers[i], throwable);
            }
        }
    }

    @Override
    public void collectException(MetricDescriptor descriptor, Exception e) {
        logger.warning("Error when collecting '" + descriptor.toString() + '\'', e);
    }

    @Override
    public void collectNoValue(MetricDescriptor descriptor) {
        // noop
    }

    private void logError(MetricDescriptor descriptor, Object value,
                          MetricsPublisher publisher, Throwable throwable) {
        logger.fine("Error publishing metric to: " + publisher.name() + ", metric=" + descriptor.toString()
                + ", value=" + value, throwable);
    }

}
