package com.hazelcast.collection;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;

/**
 * @ali 8/31/13
 */
public class CollectionItem implements Comparable<CollectionItem>, IdentifiedDataSerializable {

    private long itemId;

    private Object value;

    private final long creationTime;

    private transient CollectionContainer container;

    public CollectionItem() {
        creationTime = Clock.currentTimeMillis();
    }

    public CollectionItem(CollectionContainer container, long itemId, Object value) {
        this();
        this.container = container;
        this.itemId = itemId;
        this.value = value;
    }

    public long getItemId() {
        return itemId;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
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

        CollectionItem that = (CollectionItem) o;

        if (itemId != that.itemId) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    public int hashCode() {
        int result = (int) (itemId ^ (itemId >>> 32));
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    public int getFactoryId() {
        return CollectionDataSerializerHook.F_ID;
    }

    public int getId() {
        return CollectionDataSerializerHook.COLLECTION_ITEM;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(itemId);
        out.writeObject(value);
    }

    public void readData(ObjectDataInput in) throws IOException {
        itemId = in.readLong();
        value = in.readObject();
    }
}
