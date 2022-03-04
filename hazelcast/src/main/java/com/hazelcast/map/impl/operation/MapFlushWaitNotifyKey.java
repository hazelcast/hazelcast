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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.operationservice.AbstractWaitNotifyKey;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

/**
 * Key used to notify end of {@link IMap#flush()} operation.
 *
 * @see AwaitMapFlushOperation
 * @see NotifyMapFlushOperation
 */
public class MapFlushWaitNotifyKey extends AbstractWaitNotifyKey {

    private final int partitionId;
    private final long happenedFlushCount;

    public MapFlushWaitNotifyKey(String mapName, int partitionId, long happenedFlushCount) {
        super(SERVICE_NAME, mapName);
        this.happenedFlushCount = happenedFlushCount;
        this.partitionId = partitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        MapFlushWaitNotifyKey that = (MapFlushWaitNotifyKey) o;

        if (partitionId != that.partitionId) {
            return false;
        }

        return happenedFlushCount == that.happenedFlushCount;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + partitionId;
        result = 31 * result + (int) (happenedFlushCount ^ (happenedFlushCount >>> 32));
        return result;
    }
}
