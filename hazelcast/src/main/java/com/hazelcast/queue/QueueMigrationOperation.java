package com.hazelcast.queue;

import com.hazelcast.nio.Data;
import com.hazelcast.spi.Operation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: ali
 * Date: 11/21/12
 * Time: 11:23 AM
 * To change this template use File | Settings | File Templates.
 */
public class QueueMigrationOperation extends Operation {

    Map<String, QueueContainer> migrationData;

    public QueueMigrationOperation() {
    }

    public QueueMigrationOperation(Map<String, QueueContainer> migrationData, int partitionId, int replicaIndex) {
        setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        this.migrationData = migrationData;
    }

    public void run() {
        QueueService service = getService();
        for(Map.Entry<String, QueueContainer> entry: migrationData.entrySet()){
            String name = entry.getKey();
            service.addContainer(name, entry.getValue());
        }
    }

    protected void writeInternal(DataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for(Map.Entry<String,QueueContainer> entry: migrationData.entrySet()){
            out.writeUTF(entry.getKey());
            QueueContainer container = entry.getValue();
            out.writeInt(container.partitionId);
            out.writeInt(container.dataQueue.size());
            Iterator<Data> iterator = container.dataQueue.iterator();
            while (iterator.hasNext()){
                Data data = iterator.next();
                data.writeData(out);
            }
        }
    }

    protected void readInternal(DataInput in) throws IOException {
        int mapSize = in.readInt();
        migrationData = new HashMap<String, QueueContainer>(mapSize);
        for(int i=0; i<mapSize; i++){
            String name = in.readUTF();
            int partitionId = in.readInt();
            QueueContainer container = new QueueContainer(partitionId);
            int size = in.readInt();
            for(int j=0; j<size; j++){
                Data data = new Data();
                data.readData(in);
                container.dataQueue.offer(data);
            }
            migrationData.put(name, container);
        }
    }

    public String getServiceName() {
        return QueueService.NAME;
    }
}
