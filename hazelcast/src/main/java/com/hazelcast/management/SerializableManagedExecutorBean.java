package com.hazelcast.management;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.executor.ManagedExecutorService;
import java.io.IOException;

/**
 * A Serializable DTO for {@link com.hazelcast.jmx.ManagedExecutorServiceMBean}.
 */
public class SerializableManagedExecutorBean implements DataSerializable {

    private String name;
    private int queueSize;
    private int poolSize;
    private int remainingQueueCapacity;
    private int maximumPoolSize;
    private boolean isTerminated;
    private long completedTaskCount;

    public SerializableManagedExecutorBean() {
    }

    public SerializableManagedExecutorBean(ManagedExecutorService executorService) {
        this.name = executorService.getName();
        this.queueSize = executorService.getQueueSize();
        this.poolSize = executorService.getPoolSize();
        this.remainingQueueCapacity = executorService.getRemainingQueueCapacity();
        this.maximumPoolSize = executorService.getMaximumPoolSize();
        this.isTerminated = executorService.isTerminated();
        this.completedTaskCount = executorService.getCompletedTaskCount();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    public int getRemainingQueueCapacity() {
        return remainingQueueCapacity;
    }

    public void setRemainingQueueCapacity(int remainingQueueCapacity) {
        this.remainingQueueCapacity = remainingQueueCapacity;
    }

    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }

    public void setMaximumPoolSize(int maximumPoolSize) {
        this.maximumPoolSize = maximumPoolSize;
    }

    public boolean isTerminated() {
        return isTerminated;
    }

    public void setTerminated(boolean isTerminated) {
        this.isTerminated = isTerminated;
    }

    public long getCompletedTaskCount() {
        return completedTaskCount;
    }

    public void setCompletedTaskCount(long completedTaskCount) {
        this.completedTaskCount = completedTaskCount;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(queueSize);
        out.writeInt(poolSize);
        out.writeInt(remainingQueueCapacity);
        out.writeInt(maximumPoolSize);
        out.writeBoolean(isTerminated);
        out.writeLong(completedTaskCount);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        queueSize = in.readInt();
        poolSize = in.readInt();
        remainingQueueCapacity = in.readInt();
        maximumPoolSize = in.readInt();
        isTerminated = in.readBoolean();
        completedTaskCount = in.readLong();
    }
}
