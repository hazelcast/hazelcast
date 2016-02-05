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

package com.hazelcast.map.impl.record;

import com.hazelcast.nio.serialization.Data;

import static com.hazelcast.util.JVMUtil.OBJECT_HEADER_IN_BYTES;
import static com.hazelcast.util.JVMUtil.REFERENCE_COST_IN_BYTES;


class DataRecord extends AbstractRecord<Data> {

    protected Data value;

    DataRecord(Data keyData, Data value) {
        super(keyData);
        this.value = value;
    }

    DataRecord() {
    }

    /*
    * get record size in bytes.
    *
    * */
    @Override
    public long getCost() {
        return super.getCost() + OBJECT_HEADER_IN_BYTES
                + REFERENCE_COST_IN_BYTES + (value == null ? 0 : value.getHeapCost());
    }

    public Data getValue() {
        return value;
    }

    public void setValue(Data o) {
        value = o;
    }

    public void invalidate() {
        value = null;
    }

}
