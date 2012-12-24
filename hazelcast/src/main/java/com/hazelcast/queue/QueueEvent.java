/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.queue;

import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.IOUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ali 12/24/12
 */
public class QueueEvent implements DataSerializable {

    String name;

    Data data;

    ItemEventType eventType;

    public QueueEvent(String name, Data data, ItemEventType eventType) {
        this.name = name;
        this.data = data;
        this.eventType = eventType;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeUTF(name);
        IOUtil.writeNullableData(out, data);
        out.writeInt(eventType.getType());
    }

    public void readData(DataInput in) throws IOException {
        name = in.readUTF();
        data = IOUtil.readNullableData(in);
        eventType = ItemEventType.getByType(in.readInt());
    }
}
