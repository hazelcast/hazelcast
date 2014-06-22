package com.hazelcast.map.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.InvocationClientRequest;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.map.proxy.MapProxyImpl;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;

import java.io.IOException;
import java.security.Permission;

/**
 * Triggers the load of all keys from defined map store.
 */
public class MapLoadAllKeysRequest extends InvocationClientRequest {

    protected String name;

    private boolean replaceExistingValues;

    public MapLoadAllKeysRequest() {
    }

    public MapLoadAllKeysRequest(String name, boolean replaceExistingValues) {
        this.name = name;
        this.replaceExistingValues = replaceExistingValues;
    }

    @Override
    public void invoke() {
        setSingleConnection();
        final MapService mapService = getService();
        final DistributedObject distributedObject
                = mapService.getNodeEngine().getProxyService().getDistributedObject(MapService.SERVICE_NAME, name);
        final MapProxyImpl mapProxy = (MapProxyImpl) distributedObject;
        mapProxy.loadAll(replaceExistingValues);
        final ClientEndpoint endpoint = getEndpoint();
        endpoint.sendResponse(Boolean.TRUE, getCallId());
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapPortableHook.LOAD_ALL_KEYS;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeBoolean("r", replaceExistingValues);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        replaceExistingValues = reader.readBoolean("r");
    }
}
