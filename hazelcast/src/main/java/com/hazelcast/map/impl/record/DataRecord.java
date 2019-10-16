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

package com.hazelcast.map.impl.record;

import com.hazelcast.nio.serialization.Data;

import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;

class DataRecord extends AbstractRecord<Data> {
    protected volatile Data value;

    DataRecord(Data value) {
        this.value = value;
    }

    DataRecord() {
    }

    @Override
    public long getCost() {
        return super.getCost()
                + REFERENCE_COST_IN_BYTES
                + (value == null ? 0 : value.getHeapCost());
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
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        DataRecord that = (DataRecord) o;
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
        return "DataRecord{"
                + "value=" + value
                + ", " + super.toString()
                + "} ";
    }
}
