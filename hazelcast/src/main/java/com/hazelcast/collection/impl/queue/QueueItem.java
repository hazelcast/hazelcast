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

package com.hazelcast.collection.impl.queue;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

/**
 * Queue Item.
 */
public class QueueItem implements IdentifiedDataSerializable, Comparable<QueueItem> {

    protected long itemId;
    protected Data serializedObject;
    protected Object deserializedObject;
    protected final long creationTime;
    protected QueueContainer container;

    public QueueItem() {
        this.creationTime = Clock.currentTimeMillis();
    }

    public QueueItem(QueueContainer container, long itemId, @Nullable Data data) {
        this();
        this.container = container;
        this.itemId = itemId;
        this.serializedObject = data;
    }

    public Data getSerializedObject() {
        if (serializedObject == null && container != null) {
            serializedObject = container.getDataFromMap(itemId);
        }
        return serializedObject;
    }

    public void setSerializedObject(Data serializedObject) {
        this.serializedObject = serializedObject;
    }

    public void setContainer(QueueContainer container) {
        this.container = container;
    }

    @SuppressWarnings("unchecked")
    public <T> T getDeserializedObject() {
        if (deserializedObject == null) {
            deserializedObject = container.getSerializationService().toObject(serializedObject);
        }
        return (T) deserializedObject;
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

    public QueueContainer getContainer() {
        return container;
    }

    @Override
    public int getFactoryId() {
        return QueueDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.QUEUE_ITEM;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(itemId);
        IOUtil.writeData(out, serializedObject);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        itemId = in.readLong();
        serializedObject = IOUtil.readData(in);
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueueItem queueItem = (QueueItem) o;
        return itemId == queueItem.itemId && Objects.equals(serializedObject, queueItem.serializedObject);
    }

    @Override
    public int hashCode() {
        return Objects.hash(itemId, serializedObject);
    }
}
