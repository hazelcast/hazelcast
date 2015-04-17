/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.spi.NodeEngine;

import java.util.Collection;

/**
 * Helper class to limit query result size.
 */
public class MapQueryResultSizeLimitHelper {

    // due to the unequal data distribution we set a minimum value for the trigger to ensure a better distribution
    public static final int MINIMUM_MAX_RESULT_LIMIT = 100000;

    // due to the unequal data distribution we add a security margin to prevent false positives
    public static final float MAX_RESULT_LIMIT_FACTOR = 1.15f;

    // special value to mark the disabled state
    static final int DISABLED = -1;

    private final MapServiceContext mapServiceContext;
    private final ILogger log;

    private final int maxResultLimit;
    private final int maxLocalPartitionsLimitForPreCheck;
    private final float resultLimitPerPartition;

    private final boolean isQueryResultLimitEnabled;
    private final boolean isPreCheckEnabled;

    public MapQueryResultSizeLimitHelper(MapServiceContext mapServiceContext, ILogger log) {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();

        this.mapServiceContext = mapServiceContext;
        this.log = log;

        GroupProperties groupProperties = nodeEngine.getGroupProperties();
        this.maxResultLimit = getMaxResultLimit(groupProperties);
        this.maxLocalPartitionsLimitForPreCheck = getMaxLocalPartitionsLimitForPreCheck(groupProperties);
        this.resultLimitPerPartition = maxResultLimit * MAX_RESULT_LIMIT_FACTOR / getPartitionCount(groupProperties);

        this.isQueryResultLimitEnabled = (maxResultLimit != DISABLED);
        this.isPreCheckEnabled = (isQueryResultLimitEnabled && maxLocalPartitionsLimitForPreCheck != DISABLED);
    }

    public boolean isQueryResultLimitEnabled() {
        return isQueryResultLimitEnabled;
    }

    public long getNodeResultLimit(int ownedPartitions) {
        return (long) Math.ceil(resultLimitPerPartition * ownedPartitions);
    }

    boolean isPreCheckEnabled() {
        return isPreCheckEnabled;
    }

    void checkMaxResultLimitOnLocalPartitions(String mapName) {
        // check if feature is enabled
        if (!isPreCheckEnabled) {
            return;
        }

        // limit number of local partitions to check to keep runtime constant
        Collection<Integer> localPartitions = mapServiceContext.getOwnedPartitions();
        int partitionsToCheck = Math.min(localPartitions.size(), maxLocalPartitionsLimitForPreCheck);
        if (partitionsToCheck == 0) {
            return;
        }

        // calculate size of local partitions
        int localPartitionSize = getLocalPartitionSize(mapName, localPartitions, partitionsToCheck);
        if (localPartitionSize == 0) {
            return;
        }

        // check local result size
        long localResultLimit = getNodeResultLimit(partitionsToCheck);
        if (localPartitionSize > localResultLimit) {
            throw new QueryResultSizeExceededException(maxResultLimit, " Result size exceeded in local pre-check.");
        }
    }

    int getMaxResultLimit() {
        return maxResultLimit;
    }

    private int getLocalPartitionSize(String mapName, Collection<Integer> localPartitions, int partitionsToCheck) {
        int localSize = 0;
        int partitionsChecked = 0;
        for (int partitionId : localPartitions) {
            localSize += mapServiceContext.getRecordStore(partitionId, mapName).size();
            if (++partitionsChecked == partitionsToCheck) {
                break;
            }
        }
        return localSize;
    }

    private int getMaxResultLimit(GroupProperties groupProperties) {
        int maxResultLimit = groupProperties.QUERY_RESULT_SIZE_LIMIT.getInteger();
        if (maxResultLimit == -1) {
            return DISABLED;
        }
        if (maxResultLimit <= 0) {
            throw new IllegalArgumentException(groupProperties.QUERY_RESULT_SIZE_LIMIT.getName()
                    + " has to be -1 (disabled) or a positive number!");
        }
        if (maxResultLimit < MINIMUM_MAX_RESULT_LIMIT) {
            log.finest("Max result limit was set to minimal value of " + MINIMUM_MAX_RESULT_LIMIT);
            return MINIMUM_MAX_RESULT_LIMIT;
        }
        return maxResultLimit;
    }

    private int getMaxLocalPartitionsLimitForPreCheck(GroupProperties groupProperties) {
        int maxLocalPartitionLimitForPreCheck = groupProperties.QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK.getInteger();
        if (maxLocalPartitionLimitForPreCheck == -1) {
            return DISABLED;
        }
        if (maxLocalPartitionLimitForPreCheck <= 0) {
            throw new IllegalArgumentException(groupProperties.QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK.getName()
                    + " has to be -1 (disabled) or a positive number!");
        }
        return maxLocalPartitionLimitForPreCheck;
    }

    private float getPartitionCount(GroupProperties groupProperties) {
        int partitionCount = groupProperties.PARTITION_COUNT.getInteger();
        if (partitionCount <= 0) {
            throw new IllegalArgumentException(groupProperties.PARTITION_COUNT.getName() + " has to be a positive number!");
        }
        return groupProperties.PARTITION_COUNT.getInteger();
    }
}
