package com.hazelcast.map.impl.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.CallableClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.map.MapPartitionLostEvent;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.PortableMapPartitionLostEvent;

import java.io.IOException;
import java.security.Permission;

public class MapAddPartitionLostListenerRequest extends CallableClientRequest
        implements RetryableRequest {


    private String name;

    public MapAddPartitionLostListenerRequest() {
    }

    public MapAddPartitionLostListenerRequest(String name) {
        this.name = name;
    }

    @Override
    public Object call() {
        final ClientEndpoint endpoint = getEndpoint();
        final MapService mapService = getService();

        final MapPartitionLostListener listener = new MapPartitionLostListener() {
            @Override
            public void partitionLost(MapPartitionLostEvent event) {
                if (endpoint.isAlive()) {
                    final PortableMapPartitionLostEvent portableEvent =
                            new PortableMapPartitionLostEvent(event.getPartitionId(), event.getMember().getUuid());
                    endpoint.sendEvent(null, portableEvent, getCallId());
                }
            }
        };

        final String registrationId = mapService.getMapServiceContext().addPartitionLostListener(listener, name);
        endpoint.setListenerRegistration(MapService.SERVICE_NAME, name, registrationId);
        return registrationId;
    }


    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("name", name);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("name");
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }
    @Override
    public String getMethodName() {
        return "addPartitionLostListener";
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapPortableHook.ADD_MAP_PARTITION_LOST_LISTENER;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }
}
