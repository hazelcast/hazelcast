/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.record;

import com.hazelcast.nio.serialization.Data;

/**
 * @param <V>
 */
abstract class AbstractRecordWithStats<V> extends AbstractRecord<V> {

    protected RecordStatistics recordStatistics;

    protected AbstractRecordWithStats() {
    }

    protected AbstractRecordWithStats(Data key) {
        super(key);
        this.recordStatistics = new RecordStatistics();
    }

    public final RecordStatistics getStatistics() {
        return recordStatistics;
    }

    public final void setStatistics(RecordStatistics recordStatistics) {
        this.recordStatistics = recordStatistics;
    }

    public final void onAccess() {
        super.onAccess();
        this.recordStatistics.access();
    }

    public final void onStore() {
        this.recordStatistics.store();
    }

    public final void onUpdate() {
        super.onUpdate();
        this.recordStatistics.update();
    }

    @Override
    public long getCost() {
        final long cost = super.getCost();
        // reference cost of RecordStatistics object.
        final int objectReferenceInBytes = 4;
        return cost + objectReferenceInBytes + recordStatistics.size();
    }
}
