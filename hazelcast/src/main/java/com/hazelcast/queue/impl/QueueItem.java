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

package com.hazelcast.queue.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;

/**
 * Queue Item.
 */
public class QueueItem implements IdentifiedDataSerializable, Comparable<QueueItem> {

    protected long itemId;
    protected Data data;
    protected final long creationTime;
    protected QueueContainer container;

    public QueueItem() {
        this.creationTime = Clock.currentTimeMillis();
    }

    public QueueItem(QueueContainer container, long itemId, Data data) {
        this();
        this.container = container;
        this.itemId = itemId;
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

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(itemId);
        out.writeData(data);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        itemId = in.readLong();
        data = in.readData();
    }

    @Override
    public int compareTo(QueueItem o) {
        if (itemId < o.getItemId()) {
            return -1;
        } else if (itemId > o.getItemId()) {
            return 1;
        }
        return 0;
    }

    @Override
    public int getFactoryId() {
        return QueueDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.QUEUE_ITEM;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QueueItem)) {
            return false;
        }

        QueueItem item = (QueueItem) o;

        if (itemId != item.itemId) {
            return false;
        }
        if (data != null ? !data.equals(item.data) : item.data != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (itemId ^ (itemId >>> 32));
        result = 31 * result + (data != null ? data.hashCode() : 0);
        return result;
    }
}
