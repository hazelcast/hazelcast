package com.hazelcast.partition.client;

import com.hazelcast.client.impl.client.BaseClientRemoveListenerRequest;
import com.hazelcast.client.impl.client.ClientPortableHook;
import com.hazelcast.partition.InternalPartitionService;

import java.security.Permission;

public class RemovePartitionLostListenerRequest
        extends BaseClientRemoveListenerRequest {

    public RemovePartitionLostListenerRequest() {
    }

    public RemovePartitionLostListenerRequest(String registrationId) {
        super(null, registrationId);
    }

    public Object call()
            throws Exception {
        final InternalPartitionService service = getService();
        return service.removePartitionLostListener(registrationId);
    }

    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ClientPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return ClientPortableHook.REMOVE_PARTITION_LOST_LISTENER;
    }
    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "removePartitionLostListener";
    }
}
