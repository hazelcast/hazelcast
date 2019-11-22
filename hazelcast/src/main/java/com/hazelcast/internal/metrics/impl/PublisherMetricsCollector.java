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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsPublisher;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

/**
 * {@link MetricsCollector} implementation delegating to the configured
 * publishers.
 */
public class PublisherMetricsCollector implements MetricsCollector {
    private final ILogger logger = Logger.getLogger(PublisherMetricsCollector.class);

    private final AtomicReference<PublisherContainer> internalCollectorRef = new AtomicReference<>(new PublisherContainer());

    public synchronized void addPublisher(MetricsPublisher publisher) {
        PublisherContainer currentCollector = internalCollectorRef.get();
        MetricsPublisher[] currentPublishers = currentCollector.publishers;
        MetricsPublisher[] newPublishers = new MetricsPublisher[currentPublishers.length + 1];
        System.arraycopy(currentPublishers, 0, newPublishers, 0, currentPublishers.length);
        newPublishers[newPublishers.length - 1] = publisher;
        PublisherContainer newCollector = new PublisherContainer(newPublishers);
        internalCollectorRef.set(newCollector);
    }

    public boolean hasPublishers() {
        return internalCollectorRef.get().publishers.length != 0;
    }

    public List<MetricsPublisher> getPublishers() {
        return unmodifiableList(asList(internalCollectorRef.get().publishers));
    }

    public void publishCollectedMetrics() {
        for (MetricsPublisher publisher : publishers()) {
            try {
                publisher.whenComplete();
            } catch (Exception e) {
                logger.severe("Error completing publication for publisher " + publisher, e);
            }
        }
    }

    public void shutdown() {
        for (MetricsPublisher publisher : publishers()) {
            try {
                publisher.shutdown();
            } catch (Exception e) {
                logger.warning("Error shutting down metrics publisher " + publisher.name(), e);
            }
        }
    }

    @Override
    public void collectLong(MetricDescriptor descriptor, long value) {
        for (MetricsPublisher publisher : publishers()) {
            try {
                publisher.publishLong(descriptor, value);
            } catch (Exception e) {
                logError(descriptor, value, publisher, e);
            }
        }
    }

    @Override
    public void collectDouble(MetricDescriptor descriptor, double value) {
        for (MetricsPublisher publisher : publishers()) {
            try {
                publisher.publishDouble(descriptor, value);
            } catch (Exception e) {
                logError(descriptor, value, publisher, e);
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

    private MetricsPublisher[] publishers() {
        return internalCollectorRef.get().publishers;
    }

    private void logError(MetricDescriptor descriptor, Object value, MetricsPublisher publisher, Exception e) {
        logger.fine("Error publishing metric to: " + publisher.name() + ", metric=" + descriptor.toString()
                + ", value=" + value, e);
    }

    private static final class PublisherContainer {
        final MetricsPublisher[] publishers;

        PublisherContainer() {
            this.publishers = new MetricsPublisher[0];
        }

        PublisherContainer(MetricsPublisher[] publishers) {
            this.publishers = publishers;
        }
    }
}
