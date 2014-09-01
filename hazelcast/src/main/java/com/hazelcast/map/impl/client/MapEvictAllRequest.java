package com.hazelcast.map.impl.client;

import com.hazelcast.client.impl.client.AllPartitionsClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.EvictAllOperationFactory;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.OperationFactory;
import java.io.IOException;
import java.security.Permission;
import java.util.Map;

/**
 * Evict all entries request used by clients.
 */
public class MapEvictAllRequest extends AllPartitionsClientRequest implements Portable, RetryableRequest, SecureRequest {

    private String name;

    public MapEvictAllRequest() {
    }

    public MapEvictAllRequest(String name) {
        this.name = name;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.EVICT_ALL;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new EvictAllOperationFactory(name);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        int total = 0;
        MapService mapService = getService();
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        for (Object result : map.values()) {
            Integer size = (Integer) mapServiceContext.toObject(result);
            total += size;
        }
        final Address thisAddress = mapServiceContext.getNodeEngine().getThisAddress();
        if (total > 0) {
            mapServiceContext.getMapEventPublisher().publishMapEvent(thisAddress, name, EntryEventType.EVICT_ALL, total);
        }
        return total;
    }

    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        return "evictAll";
    }
}
