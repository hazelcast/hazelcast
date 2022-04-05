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

package com.hazelcast.map.impl.query;

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.internal.util.collection.PartitionIdSet;

import java.util.PrimitiveIterator;

import static com.hazelcast.spi.properties.ClusterProperty.QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK;
import static com.hazelcast.spi.properties.ClusterProperty.QUERY_RESULT_SIZE_LIMIT;
import static java.lang.Math.ceil;
import static java.lang.Math.min;

/**
 * Responsible for limiting result size of queries.
 * <p>
 * This class defines a hard coded minimum {@link #MINIMUM_MAX_RESULT_LIMIT} as well as a factor {@link #MAX_RESULT_LIMIT_FACTOR}
 * to ensure that the actual result size limit will be a reasonable value. Due to the used hash algorithm the data of
 * a map is not distributed equally on all partitions in the cluster. Since the decision whether of not the
 * {@link QueryResultSizeExceededException} is thrown is made on the local number of partition entries, we need a reliable
 * distribution of data. The goal is to prevent false positives, since those might surprise the user.
 * So the exception should never been thrown below the configured limit.
 * <p>
 * The minimum value of {@value #MINIMUM_MAX_RESULT_LIMIT} and the factor of {@value #MAX_RESULT_LIMIT_FACTOR} were determined by
 * testing on which limit the exception was thrown with different map key types.
 */
public class QueryResultSizeLimiter {

    /**
     * Defines the minimum value for the result size limit to ensure a sufficient distribution of data in the partitions.
     *
     * @see QueryResultSizeLimiter
     */
    public static final int MINIMUM_MAX_RESULT_LIMIT = 100000;

    /**
     * Adds a security margin to the configured result size limit to prevent false positives.
     *
     * @see QueryResultSizeLimiter
     */
    public static final float MAX_RESULT_LIMIT_FACTOR = 1.15f;

    /**
     * Adds a security margin to the configured result size limit to prevent false positives in pre-check.
     * Required since the precheck takes 3 partitions by default and their size may vary due to data distr. imbalance.
     *
     * @see QueryResultSizeLimiter
     */
    public static final float MAX_RESULT_LIMIT_FACTOR_FOR_PRECHECK = 1.25f;

    /**
     * Special value to mark the disabled state.
     */
    static final int DISABLED = -1;

    private final MapServiceContext mapServiceContext;
    private final ILogger logger;

    private final int maxResultLimit;
    private final int maxLocalPartitionsLimitForPreCheck;
    private final float resultLimitPerPartition;

    private final boolean isQueryResultLimitEnabled;
    private final boolean isPreCheckEnabled;

    public QueryResultSizeLimiter(MapServiceContext mapServiceContext, ILogger logger) {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();

        this.mapServiceContext = mapServiceContext;
        this.logger = logger;

        HazelcastProperties hazelcastProperties = nodeEngine.getProperties();
        this.maxResultLimit = getMaxResultLimit(hazelcastProperties);
        this.maxLocalPartitionsLimitForPreCheck = getMaxLocalPartitionsLimitForPreCheck(hazelcastProperties);
        this.resultLimitPerPartition = maxResultLimit * MAX_RESULT_LIMIT_FACTOR / (float) getPartitionCount(nodeEngine);

        this.isQueryResultLimitEnabled = (maxResultLimit != DISABLED);
        this.isPreCheckEnabled = (isQueryResultLimitEnabled && maxLocalPartitionsLimitForPreCheck != DISABLED);
    }

    /**
     * Just for testing.
     */
    public boolean isQueryResultLimitEnabled() {
        return isQueryResultLimitEnabled;
    }

    /**
     * Just for testing.
     */
    public boolean isPreCheckEnabled() {
        return isPreCheckEnabled;
    }

    long getNodeResultLimit(int ownedPartitions) {
        return isQueryResultLimitEnabled ? (long) ceil(resultLimitPerPartition * ownedPartitions) : Long.MAX_VALUE;
    }

    void precheckMaxResultLimitOnLocalPartitions(String mapName) {
        // check if feature is enabled
        if (!isPreCheckEnabled) {
            return;
        }

        // limit number of local partitions to check to keep runtime constant
        PartitionIdSet localPartitions = mapServiceContext.getOrInitCachedMemberPartitions();
        int partitionsToCheck = min(localPartitions.size(), maxLocalPartitionsLimitForPreCheck);
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
        if (localPartitionSize > localResultLimit * MAX_RESULT_LIMIT_FACTOR_FOR_PRECHECK) {
            throw new QueryResultSizeExceededException(maxResultLimit, " Result size exceeded in local pre-check.");
        }
    }

    private int getLocalPartitionSize(String mapName, PartitionIdSet localPartitions, int partitionsToCheck) {
        int localSize = 0;
        int partitionsChecked = 0;
        PrimitiveIterator.OfInt partitionsIterator = localPartitions.intIterator();
        while (partitionsIterator.hasNext()) {
            int partitionId = partitionsIterator.nextInt();
            localSize += mapServiceContext.getRecordStore(partitionId, mapName).size();
            if (++partitionsChecked == partitionsToCheck) {
                break;
            }
        }
        return localSize;
    }

    private int getMaxResultLimit(HazelcastProperties hazelcastProperties) {
        int maxResultLimit = hazelcastProperties.getInteger(QUERY_RESULT_SIZE_LIMIT);

        if (maxResultLimit == -1) {
            return DISABLED;
        }
        if (maxResultLimit <= 0) {
            throw new IllegalArgumentException(QUERY_RESULT_SIZE_LIMIT + " has to be -1 (disabled) or a positive number!");
        }
        if (maxResultLimit < MINIMUM_MAX_RESULT_LIMIT) {
            logger.finest("Max result limit was set to minimal value of " + MINIMUM_MAX_RESULT_LIMIT);
            return MINIMUM_MAX_RESULT_LIMIT;
        }
        return maxResultLimit;
    }

    private int getMaxLocalPartitionsLimitForPreCheck(HazelcastProperties hazelcastProperties) {
        int maxLocalPartitionLimitForPreCheck = hazelcastProperties.getInteger(QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK);
        if (maxLocalPartitionLimitForPreCheck == -1) {
            return DISABLED;
        }
        if (maxLocalPartitionLimitForPreCheck <= 0) {
            throw new IllegalArgumentException(QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK
                    + " has to be -1 (disabled) or a positive number!");
        }
        return maxLocalPartitionLimitForPreCheck;
    }

    private int getPartitionCount(NodeEngine nodeEngine) {
        return nodeEngine.getPartitionService().getPartitionCount();
    }
}
