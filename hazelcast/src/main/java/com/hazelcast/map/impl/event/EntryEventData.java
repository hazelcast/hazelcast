/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.internal.serialization.Data;

import java.io.IOException;

/**
 * An entry's event data.
 */
@BinaryInterface
public class EntryEventData extends AbstractEventData {

    protected Data dataKey;
    protected Object dataNewValue;
    protected Object dataOldValue;
    protected Object dataMergingValue;

    public EntryEventData() {
    }

    public EntryEventData(String source, String mapName, Address caller,
                          Data dataKey, Object dataNewValue, Object dataOldValue, int eventType) {
        super(source, mapName, caller, eventType);
        this.dataKey = dataKey;
        this.dataNewValue = dataNewValue;
        this.dataOldValue = dataOldValue;
    }

    public EntryEventData(String source, String mapName, Address caller,
                          Data dataKey, Object dataNewValue, Object dataOldValue, Object dataMergingValue, int eventType) {
        super(source, mapName, caller, eventType);
        this.dataKey = dataKey;
        this.dataNewValue = dataNewValue;
        this.dataOldValue = dataOldValue;
        this.dataMergingValue = dataMergingValue;
    }

    public Data getDataKey() {
        return dataKey;
    }

    public Object getDataNewValue() {
        return dataNewValue;
    }

    public Object getDataOldValue() {
        return dataOldValue;
    }

    public Object getDataMergingValue() {
        return dataMergingValue;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        IOUtil.writeData(out, dataKey);
        out.writeObject(dataNewValue);
        out.writeObject(dataOldValue);
        out.writeObject(dataMergingValue);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        dataKey = IOUtil.readData(in);
        dataNewValue = in.readObject();
        dataOldValue = in.readObject();
        dataMergingValue = in.readObject();
    }

    @Override
    public String toString() {
        return "EntryEventData{"
                + super.toString()
                + '}';
    }
}
