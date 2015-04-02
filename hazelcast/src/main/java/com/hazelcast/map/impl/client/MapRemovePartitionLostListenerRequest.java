package com.hazelcast.map.impl.client;

import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;

import java.security.Permission;

public class MapRemovePartitionLostListenerRequest  extends BaseClientRemoveListenerRequest {


    public MapRemovePartitionLostListenerRequest() {
    }

    public MapRemovePartitionLostListenerRequest(String name, String registrationId) {
        super(name, registrationId);
    }

    public Object call() throws Exception {
        final MapService service = getService();
        return service.getMapServiceContext().removePartitionLostListener(name, registrationId);
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.REMOVE_MAP_PARTITION_LOST_LISTENER;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getMethodName() {
        return "removePartitionLostListener";
    }
}
