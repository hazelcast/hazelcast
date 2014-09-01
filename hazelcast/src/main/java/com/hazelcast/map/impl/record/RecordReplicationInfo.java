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

package com.hazelcast.map.impl.record;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import java.io.IOException;

public class RecordReplicationInfo extends RecordInfo implements DataSerializable {

    private Data key;
    private Data value;

    public RecordReplicationInfo(Data key, Data value, RecordInfo recordInfo) {
        super(recordInfo);
        this.key = key;
        this.value = value;
    }


    public RecordReplicationInfo() {
    }

    public Data getKey() {
        return key;
    }

    public Data getValue() {
        return value;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        key.writeData(out);
        value.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        key = new Data();
        key.readData(in);
        value = new Data();
        value.readData(in);
    }

    @Override
    public String toString() {
        return "RecordReplicationInfo{"
                + "key=" + key
                + ", value=" + value
                + "} " + super.toString();
    }
}
