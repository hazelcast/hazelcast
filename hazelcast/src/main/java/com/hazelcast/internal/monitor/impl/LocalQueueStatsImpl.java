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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.collection.LocalQueueStats;
import com.hazelcast.internal.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.util.JsonUtil.getInt;
import static com.hazelcast.internal.util.JsonUtil.getLong;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public class LocalQueueStatsImpl implements LocalQueueStats {

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

    @Probe
    private int ownedItemCount;
    @Probe
    private int backupItemCount;
    @Probe
    private long minAge;
    @Probe
    private long maxAge;
    @Probe
    private long aveAge;
    @Probe
    private long creationTime;

    // These fields are only accessed through the updater
    @Probe
    private volatile long numberOfOffers;
    @Probe
    private volatile long numberOfRejectedOffers;
    @Probe
    private volatile long numberOfPolls;
    @Probe
    private volatile long numberOfEmptyPolls;
    @Probe
    private volatile long numberOfOtherOperations;
    @Probe
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
    public long getAvgAge() {
        return aveAge;
    }

    public void setAveAge(long aveAge) {
        this.aveAge = aveAge;
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

    @Probe
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

    @Probe
    @Override
    public long getEventOperationCount() {
        return numberOfEvents;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("ownedItemCount", ownedItemCount);
        root.add("backupItemCount", backupItemCount);
        root.add("minAge", minAge);
        root.add("maxAge", maxAge);
        root.add("aveAge", aveAge);
        root.add("creationTime", creationTime);
        root.add("numberOfOffers", numberOfOffers);
        root.add("numberOfPolls", numberOfPolls);
        root.add("numberOfRejectedOffers", numberOfRejectedOffers);
        root.add("numberOfEmptyPolls", numberOfEmptyPolls);
        root.add("numberOfOtherOperations", numberOfOtherOperations);
        root.add("numberOfEvents", numberOfEvents);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        ownedItemCount = getInt(json, "ownedItemCount", -1);
        backupItemCount = getInt(json, "backupItemCount", -1);
        minAge = getLong(json, "minAge", -1L);
        maxAge = getLong(json, "maxAge", -1L);
        aveAge = getLong(json, "aveAge", -1L);
        creationTime = getLong(json, "creationTime", -1L);
        NUMBER_OF_OFFERS.set(this, getLong(json, "numberOfOffers", -1L));
        NUMBER_OF_POLLS.set(this, getLong(json, "numberOfPolls", -1L));
        NUMBER_OF_REJECTED_OFFERS.set(this, getLong(json, "numberOfRejectedOffers", -1L));
        NUMBER_OF_EMPTY_POLLS.set(this, getLong(json, "numberOfEmptyPolls", -1L));
        NUMBER_OF_OTHER_OPERATIONS.set(this, getLong(json, "numberOfOtherOperations", -1L));
        NUMBER_OF_EVENTS.set(this, getLong(json, "numberOfEvents", -1L));
    }

    @Override
    public String toString() {
        return "LocalQueueStatsImpl{"
                + "ownedItemCount=" + ownedItemCount
                + ", backupItemCount=" + backupItemCount
                + ", minAge=" + minAge
                + ", maxAge=" + maxAge
                + ", aveAge=" + aveAge
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
