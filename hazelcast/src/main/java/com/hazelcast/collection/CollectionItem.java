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

package com.hazelcast.collection;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Clock;
import java.io.IOException;

public class CollectionItem implements Comparable<CollectionItem>, IdentifiedDataSerializable {

    protected long itemId;

    protected Data value;

    protected final long creationTime;

    public CollectionItem() {
        creationTime = Clock.currentTimeMillis();
    }

    public CollectionItem(long itemId, Data value) {
        this();
        this.itemId = itemId;
        this.value = value;
    }

    public long getItemId() {
        return itemId;
    }

    public Data getValue() {
        return value;
    }

    public void setValue(Data value) {
        this.value = value;
    }

    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public int compareTo(CollectionItem o) {
        long otherItemId = o.getItemId();
        if (itemId > otherItemId) {
            return 1;
        } else if (itemId < otherItemId) {
            return -1;
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CollectionItem)) {
            return false;
        }

        CollectionItem item = (CollectionItem) o;

        if (value != null ? !value.equals(item.value) : item.value != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    @Override
    public int getFactoryId() {
        return CollectionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_ITEM;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(itemId);
        out.writeObject(value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        itemId = in.readLong();
        value = in.readObject();
    }
}
