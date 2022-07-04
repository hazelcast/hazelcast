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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.collection.LocalQueueStats;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.QUEUE_METRIC_AVERAGE_AGE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.QUEUE_METRIC_BACKUP_ITEM_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.QUEUE_METRIC_CREATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.QUEUE_METRIC_EVENT_OPERATION_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.QUEUE_METRIC_MAX_AGE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.QUEUE_METRIC_MIN_AGE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.QUEUE_METRIC_NUMBER_OF_EMPTY_POLLS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.QUEUE_METRIC_NUMBER_OF_EVENTS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.QUEUE_METRIC_NUMBER_OF_OFFERS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.QUEUE_METRIC_NUMBER_OF_OTHER_OPERATIONS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.QUEUE_METRIC_NUMBER_OF_POLLS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.QUEUE_METRIC_NUMBER_OF_REJECTED_OFFERS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.QUEUE_METRIC_OWNED_ITEM_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.QUEUE_METRIC_TOTAL;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public class LocalQueueStatsImpl implements LocalQueueStats {

    public static final long DEFAULT_MAX_AGE = 0;
    public static final long DEFAULT_MIN_AGE = Long.MAX_VALUE;
    private static final AtomicLongFieldUpdater<LocalQueueStatsImpl> NUMBER_OF_OFFERS =
            newUpdater(LocalQueueStatsImpl.class, "numberOfOffers");
    private static final AtomicLongFieldUpdater<LocalQueueStatsImpl> NUMBER_OF_REJECTED_OFFERS =
            newUpdater(LocalQueueStatsImpl.class, "numberOfRejectedOffers");
    private static final AtomicLongFieldUpdater<LocalQueueStatsImpl> NUMBER_OF_POLLS =
            newUpdater(LocalQueueStatsImpl.class, "numberOfPolls");
    private static final AtomicLongFieldUpdater<LocalQueueStatsImpl> NUMBER_OF_EMPTY_POLLS =
            newUpdater(LocalQueueStatsImpl.class, "numberOfEmptyPolls");
    private static final AtomicLongFieldUpdater<LocalQueueStatsImpl> NUMBER_OF_OTHER_OPERATIONS =
            newUpdater(LocalQueueStatsImpl.class, "numberOfOtherOperations");
    private static final AtomicLongFieldUpdater<LocalQueueStatsImpl> NUMBER_OF_EVENTS =
            newUpdater(LocalQueueStatsImpl.class, "numberOfEvents");

    @Probe(name = QUEUE_METRIC_OWNED_ITEM_COUNT)
    private int ownedItemCount;
    @Probe(name = QUEUE_METRIC_BACKUP_ITEM_COUNT)
    private int backupItemCount;
    @Probe(name = QUEUE_METRIC_MIN_AGE, unit = MS)
    private long minAge = DEFAULT_MIN_AGE;
    @Probe(name = QUEUE_METRIC_MAX_AGE, unit = MS)
    private long maxAge = DEFAULT_MAX_AGE;
    @Probe(name = QUEUE_METRIC_AVERAGE_AGE, unit = MS)
    private long averageAge;
    @Probe(name = QUEUE_METRIC_CREATION_TIME, unit = MS)
    private final long creationTime;

    // These fields are only accessed through the updater
    @Probe(name = QUEUE_METRIC_NUMBER_OF_OFFERS)
    private volatile long numberOfOffers;
    @Probe(name = QUEUE_METRIC_NUMBER_OF_REJECTED_OFFERS)
    private volatile long numberOfRejectedOffers;
    @Probe(name = QUEUE_METRIC_NUMBER_OF_POLLS)
    private volatile long numberOfPolls;
    @Probe(name = QUEUE_METRIC_NUMBER_OF_EMPTY_POLLS)
    private volatile long numberOfEmptyPolls;
    @Probe(name = QUEUE_METRIC_NUMBER_OF_OTHER_OPERATIONS)
    private volatile long numberOfOtherOperations;
    @Probe(name = QUEUE_METRIC_NUMBER_OF_EVENTS)
    private volatile long numberOfEvents;

    public LocalQueueStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public long getMinAge() {
        return minAge;
    }

    public void setMinAge(long minAge) {
        this.minAge = minAge;
    }

    @Override
    public long getMaxAge() {
        return maxAge;
    }

    public void setMaxAge(long maxAge) {
        this.maxAge = maxAge;
    }

    @Override
    public long getAverageAge() {
        return averageAge;
    }

    public void setAverageAge(long averageAge) {
        this.averageAge = averageAge;
    }

    @Override
    public long getOwnedItemCount() {
        return ownedItemCount;
    }

    public void setOwnedItemCount(int ownedItemCount) {
        this.ownedItemCount = ownedItemCount;
    }

    @Override
    public long getBackupItemCount() {
        return backupItemCount;
    }

    public void setBackupItemCount(int backupItemCount) {
        this.backupItemCount = backupItemCount;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Probe(name = QUEUE_METRIC_TOTAL)
    public long total() {
        return numberOfOffers + numberOfPolls + numberOfOtherOperations;
    }

    @Override
    public long getOfferOperationCount() {
        return numberOfOffers;
    }

    @Override
    public long getRejectedOfferOperationCount() {
        return numberOfRejectedOffers;
    }

    @Override
    public long getPollOperationCount() {
        return numberOfPolls;
    }

    @Override
    public long getEmptyPollOperationCount() {
        return numberOfEmptyPolls;
    }

    @Override
    public long getOtherOperationsCount() {
        return numberOfOtherOperations;
    }

    public void incrementOtherOperations() {
        NUMBER_OF_OTHER_OPERATIONS.incrementAndGet(this);
    }

    public void incrementOffers() {
        NUMBER_OF_OFFERS.incrementAndGet(this);
    }

    public void incrementRejectedOffers() {
        NUMBER_OF_REJECTED_OFFERS.incrementAndGet(this);
    }

    public void incrementPolls() {
        NUMBER_OF_POLLS.incrementAndGet(this);
    }

    public void incrementEmptyPolls() {
        NUMBER_OF_EMPTY_POLLS.incrementAndGet(this);
    }

    public void incrementReceivedEvents() {
        NUMBER_OF_EVENTS.incrementAndGet(this);
    }

    @Probe(name = QUEUE_METRIC_EVENT_OPERATION_COUNT)
    @Override
    public long getEventOperationCount() {
        return numberOfEvents;
    }

    @Override
    public String toString() {
        return "LocalQueueStatsImpl{"
                + "ownedItemCount=" + ownedItemCount
                + ", backupItemCount=" + backupItemCount
                + ", minAge=" + minAge
                + ", maxAge=" + maxAge
                + ", averageAge=" + averageAge
                + ", creationTime=" + creationTime
                + ", numberOfOffers=" + numberOfOffers
                + ", numberOfRejectedOffers=" + numberOfRejectedOffers
                + ", numberOfPolls=" + numberOfPolls
                + ", numberOfEmptyPolls=" + numberOfEmptyPolls
                + ", numberOfOtherOperations=" + numberOfOtherOperations
                + ", numberOfEvents=" + numberOfEvents
                + '}';
    }
}
