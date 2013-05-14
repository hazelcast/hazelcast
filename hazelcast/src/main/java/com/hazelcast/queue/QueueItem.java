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

package com.hazelcast.queue;

import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;

/**
 * @ali 12/12/12
 */
public class QueueItem implements IdentifiedDataSerializable, Comparable<QueueItem> {

    private long itemId;

    private Data data;

    private long creationTime;

    private transient QueueContainer container;

    public QueueItem() {
    }

    QueueItem(QueueContainer container) {
        this.container = container;
        this.creationTime = Clock.currentTimeMillis();
    }

    QueueItem(QueueContainer container, long itemId) {
        this(container);
        this.itemId = itemId;
    }

    public QueueItem(QueueContainer container, long itemId, Data data) {
        this(container, itemId);
        this.data = data;
    }

    public Data getData() {
        if (data == null && container != null) {
            data = container.getDataFromMap(itemId);
        }
        return data;
    }

    public void setData(Data data) {
        this.data = data;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public boolean equals(Object obj) {
        if (obj instanceof QueueItem) {
            QueueItem other = (QueueItem) obj;
            if (itemId == -1 || other.getItemId() == -1) {
                return getData() != null && data.equals(other.getData());
            }
            return itemId == other.getItemId();
        } else if (obj instanceof Data) {
            return getData() != null && data.equals(obj);
        } else if (obj instanceof Long) {
            return itemId == (Long) obj;
        }
        return false;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(itemId);
        IOUtil.writeNullableData(out, data);
    }

    public void readData(ObjectDataInput in) throws IOException {
        itemId = in.readLong();
        data = IOUtil.readNullableData(in);
    }

    public int hashCode() {
        int result = (int) (itemId ^ (itemId >>> 32));
        result = 31 * result + (data != null ? data.hashCode() : 0);
        return result;
    }

    public int compareTo(QueueItem o) {
        if (itemId < o.getItemId()){
            return -1;
        }
        else if (itemId > o.getItemId()){
            return 1;
        }
        return 0;
    }

    public int getFactoryId() {
        return QueueDataSerializerHook.F_ID;
    }

    public int getId() {
        return QueueDataSerializerHook.QUEUE_ITEM;
    }
}
