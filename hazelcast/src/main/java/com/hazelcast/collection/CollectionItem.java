package com.hazelcast.collection;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;

/**
 * @ali 8/31/13
 */
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

    public int compareTo(CollectionItem o) {
        long otherItemId = o.getItemId();
        if (itemId > otherItemId){
            return 1;
        } else if (itemId < otherItemId){
            return -1;
        }
        return 0;
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CollectionItem)) return false;

        CollectionItem item = (CollectionItem) o;

        if (value != null ? !value.equals(item.value) : item.value != null) return false;

        return true;
    }

    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    public int getFactoryId() {
        return CollectionDataSerializerHook.F_ID;
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_ITEM;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(itemId);
        value.writeData(out);
    }

    public void readData(ObjectDataInput in) throws IOException {
        itemId = in.readLong();
        value = new Data();
        value.readData(in);
    }
}
