package com.hazelcast.collection;

import com.hazelcast.collection.list.ListService;
import com.hazelcast.collection.set.SetService;

public enum CollectionType {

    List(ListService.SERVICE_NAME, (byte) 0),
    Set(SetService.SERVICE_NAME, (byte) 1);

    private final String serviceName;
    private final byte type;

    private CollectionType(String serviceName, byte type) {
        this.serviceName = serviceName;
        this.type = type;
    }

    public String getServiceName() {
        return serviceName;
    }

    public byte asByte() {
        return type;
    }

    public static CollectionType fromServiceName(String serviceName) {
        if (serviceName == null) {
            throw new NullPointerException("serviceName can't be null");
        }

        if (SetService.SERVICE_NAME.equals(serviceName)) {
            return Set;
        } else if (ListService.SERVICE_NAME.equals(serviceName)) {
            return List;
        } else {
            throw new IllegalArgumentException("Unrecognized collection service-name: " + serviceName);
        }
    }

    public static CollectionType fromByte(byte type) {
        switch (type) {
            case 0:
                return List;
            case 1:
                return Set;
            default:
                throw new IllegalArgumentException("Unregonized CollectionType:" + type);
        }
    }
}
