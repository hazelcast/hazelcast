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

package com.hazelcast.map.impl;

import com.hazelcast.internal.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.map.impl.operation.BasePutOperation;
import com.hazelcast.map.impl.operation.BaseRemoveOperation;
import com.hazelcast.map.impl.operation.GetOperation;
import com.hazelcast.map.impl.operation.SetOperation;
import com.hazelcast.map.impl.tx.TxnDeleteOperation;
import com.hazelcast.map.impl.tx.TxnSetOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

/**
 * Helper to update map operation stats on caller side.
 */
public final class MapOperationStatsUpdater {

    private MapOperationStatsUpdater() {
    }

    /**
     * Updates stats upon operation-call
     * on {@link com.hazelcast.map.IMap}
     */
    public static void incrementOperationStats(Operation operation,
                                               LocalMapStatsImpl localMapStats,
                                               long startTimeNanos) {

        long durationNanos = Timer.nanosElapsed(startTimeNanos);

        if (operation instanceof SetOperation) {
            localMapStats.incrementSetLatencyNanos(durationNanos);
            return;
        }

        if (operation instanceof BasePutOperation) {
            localMapStats.incrementPutLatencyNanos(durationNanos);
            return;
        }

        if (operation instanceof BaseRemoveOperation) {
            localMapStats.incrementRemoveLatencyNanos(durationNanos);
            return;
        }

        if (operation instanceof GetOperation) {
            localMapStats.incrementGetLatencyNanos(durationNanos);
            return;
        }
    }

    /**
     * Updates stats upon operation-call on {@link
     * com.hazelcast.transaction.TransactionalMap}
     */
    public static void incrementTxnOperationStats(Operation operation,
                                               LocalMapStatsImpl localMapStats,
                                               long startTimeNanos) {

        long durationNanos = Timer.nanosElapsed(startTimeNanos);

        if (operation instanceof TxnSetOperation) {
            localMapStats.incrementSetLatencyNanos(durationNanos);
            return;
        }

        if (operation instanceof TxnDeleteOperation) {
            localMapStats.incrementRemoveLatencyNanos(durationNanos);
            return;
        }

        if (operation instanceof GetOperation) {
            localMapStats.incrementGetLatencyNanos(durationNanos);
            return;
        }
    }
}
