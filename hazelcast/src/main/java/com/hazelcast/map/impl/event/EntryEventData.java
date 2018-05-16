/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.event;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * An entry's event data.
 */
@BinaryInterface
public class EntryEventData extends AbstractEventData {

    protected Data dataKey;
    protected Data dataNewValue;
    protected Data dataOldValue;
    protected Data dataMergingValue;

    public EntryEventData() {
    }

    public EntryEventData(String source, String mapName, Address caller,
                          Data dataKey, Data dataNewValue, Data dataOldValue, int eventType) {
        super(source, mapName, caller, eventType);
        this.dataKey = dataKey;
        this.dataNewValue = dataNewValue;
        this.dataOldValue = dataOldValue;
    }

    public EntryEventData(String source, String mapName, Address caller,
                          Data dataKey, Data dataNewValue, Data dataOldValue, Data dataMergingValue, int eventType) {
        super(source, mapName, caller, eventType);
        this.dataKey = dataKey;
        this.dataNewValue = dataNewValue;
        this.dataOldValue = dataOldValue;
        this.dataMergingValue = dataMergingValue;
    }

    public Data getDataKey() {
        return dataKey;
    }

    public Data getDataNewValue() {
        return dataNewValue;
    }

    public Data getDataOldValue() {
        return dataOldValue;
    }

    public Data getDataMergingValue() {
        return dataMergingValue;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeData(dataKey);
        out.writeData(dataNewValue);
        out.writeData(dataOldValue);
        out.writeData(dataMergingValue);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        dataKey = in.readData();
        dataNewValue = in.readData();
        dataOldValue = in.readData();
        dataMergingValue = in.readData();
    }

    @Override
    public String toString() {
        return "EntryEventData{"
                + super.toString()
                + '}';
    }
}
