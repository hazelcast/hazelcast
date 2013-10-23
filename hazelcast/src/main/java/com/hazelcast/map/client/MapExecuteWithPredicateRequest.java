package com.hazelcast.map.client;

import com.hazelcast.client.AllPartitionsClientRequest;
import com.hazelcast.client.InitializingObjectRequest;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.map.operation.PartitionWideEntryWithPredicateOperationFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * date: 9/16/13
 * author: eminn
 */
public class MapExecuteWithPredicateRequest extends AllPartitionsClientRequest implements Portable, InitializingObjectRequest {
    private String name;
    private EntryProcessor processor;
    private Predicate predicate;

    public MapExecuteWithPredicateRequest() {
    }

    public MapExecuteWithPredicateRequest(String name, EntryProcessor processor, Predicate predicate) {
        this.name = name;
        this.processor = processor;
        this.predicate = predicate;
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new PartitionWideEntryWithPredicateOperationFactory(name, processor, predicate);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        MapEntrySet result = new MapEntrySet();
        MapService mapService = getService();
        for (Object o : map.values()) {
            if (o != null) {
                MapEntrySet entrySet = (MapEntrySet)mapService.toObject(o);
                Set<Map.Entry<Data,Data>> entries = entrySet.getEntrySet();
                for (Map.Entry<Data, Data> entry : entries) {
                    result.add(entry);
                }
            }
        }
        return result;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public String getObjectName() {
        return name;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.EXECUTE_WITH_PREDICATE;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(processor);
        out.writeObject(predicate);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        final ObjectDataInput in = reader.getRawDataInput();
        processor = in.readObject();
        predicate = in.readObject();
    }
}
