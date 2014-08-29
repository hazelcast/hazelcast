package com.hazelcast.queue;

import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.core.Member;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

public class DataAwareItemEvent extends ItemEvent {

    protected final Data dataItem;
    private final transient SerializationService serializationService;

    public DataAwareItemEvent(String name, ItemEventType itemEventType, Data dataItem, Member member, SerializationService serializationService) {
        super(name, itemEventType, null, member);
        this.dataItem = dataItem;
        this.serializationService = serializationService;
    }

    public Object getItem() {
        if (item == null && dataItem != null) {
            item = serializationService.toObject(dataItem);
        }
        return item;
    }

    public Data getItemData() {
        return dataItem;
    }

}
