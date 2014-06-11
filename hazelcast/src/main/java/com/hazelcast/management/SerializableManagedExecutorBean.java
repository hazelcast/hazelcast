package com.hazelcast.management;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.util.executor.ManagedExecutorService;

import static com.hazelcast.util.JsonUtil.getBoolean;
import static com.hazelcast.util.JsonUtil.getInt;
import static com.hazelcast.util.JsonUtil.getLong;
import static com.hazelcast.util.JsonUtil.getString;

/**
 * A Serializable DTO for {@link com.hazelcast.jmx.ManagedExecutorServiceMBean}.
 */
public class SerializableManagedExecutorBean implements JsonSerializable {

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
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();
        root.add("name", name);
        root.add("queueSize", queueSize);
        root.add("poolSize", poolSize);
        root.add("remainingQueueCapacity", remainingQueueCapacity);
        root.add("maximumPoolSize", maximumPoolSize);
        root.add("isTerminated", isTerminated);
        root.add("completedTaskCount", completedTaskCount);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        name = getString(json, "name");
        queueSize = getInt(json, "queueSize");
        poolSize = getInt(json, "poolSize");
        remainingQueueCapacity = getInt(json, "remainingQueueCapacity");
        maximumPoolSize = getInt(json, "maximumPoolSize");
        isTerminated = getBoolean(json, "isTerminated");
        completedTaskCount = getLong(json, "completedTaskCount");
    }
}
