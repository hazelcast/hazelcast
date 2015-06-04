package com.hazelcast.map.impl.operation;

import com.hazelcast.core.PartitionAware;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class MapDestroyLocalPartitionContainerOperation extends Operation implements Runnable, PartitionAware<Object> {
    private final transient PartitionContainer container;
    private final transient String mapName;

    public MapDestroyLocalPartitionContainerOperation(PartitionContainer container, String mapName) {
        this.container = container;
        this.mapName = mapName;
    }

    @Override
    public Object getPartitionKey() {
        return container.getPartitionId();
    }

    @Override
    public void beforeRun() throws Exception {

    }

    public void run() {
        container.destroyMap(mapName);
    }

    @Override
    public void afterRun() throws Exception {

    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public Object getResponse() {
        return null;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {

    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {

    }
}
