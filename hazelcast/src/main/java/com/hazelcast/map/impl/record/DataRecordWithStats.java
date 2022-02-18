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

package com.hazelcast.map.impl.record;

import com.hazelcast.internal.serialization.Data;

import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;

class DataRecordWithStats extends AbstractRecord<Data> {
    protected volatile Data value;

    DataRecordWithStats() {
    }

    DataRecordWithStats(Data value) {
        setValue(value);
    }

    /**
     * Get record size in bytes.
     */
    @Override
    public long getCost() {
        return super.getCost() + REFERENCE_COST_IN_BYTES
                + (value == null ? 0L : value.getHeapCost());
    }

    @Override
    public Data getValue() {
        return value;
    }

    @Override
    public void setValue(Data o) {
        value = o;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }

        DataRecordWithStats that = (DataRecordWithStats) o;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "DataRecordWithStats{"
                + "value=" + value
                + ", " + super.toString()
                + "} ";
    }
}
