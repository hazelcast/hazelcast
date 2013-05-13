package com.hazelcast.partition.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.PartitionDataSerializerHook;
import com.hazelcast.partition.PartitionServiceImpl;

import java.io.IOException;

/**
 * @mdogan 5/13/13
 */
public final class AddMigrationListenerRequest extends CallableClientRequest implements IdentifiedDataSerializable {

    @Override
    public Object call() throws Exception {
        PartitionServiceImpl service = getService();
        service.addMigrationListener(new MigrationListener() {
            public void migrationStarted(MigrationEvent migrationEvent) {

            }

            public void migrationCompleted(MigrationEvent migrationEvent) {

            }

            public void migrationFailed(MigrationEvent migrationEvent) {

            }
        });
        return Boolean.TRUE;
    }

    @Override
    public String getServiceName() {
        return PartitionServiceImpl.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.ADD_LISTENER;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

    }
}
