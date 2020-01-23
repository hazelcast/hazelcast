package com.hazelcast.config;

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.log.encoders.DoubleEncoder;
import com.hazelcast.log.encoders.Encoder;
import com.hazelcast.log.encoders.HeapDataEncoder;
import com.hazelcast.log.encoders.IntEncoder;
import com.hazelcast.log.encoders.LongEncoder;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.Preconditions.checkAsyncBackupCount;
import static com.hazelcast.internal.util.Preconditions.checkBackupCount;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

public class LogConfig implements NamedConfig, IdentifiedDataSerializable {
    /**
     * Default value for the synchronous backup count.
     */
    public static final int DEFAULT_SYNC_BACKUP_COUNT = 1;

    /**
     * Default value of the asynchronous backup count.
     */
    public static final int DEFAULT_ASYNC_BACKUP_COUNT = 0;
    public static final int DEFAULT_SEGMENT_SIZE = 4 * 1024 * 1024;

    private String name;
    private Encoder encoder = HeapDataEncoder.INSTANCE;
    private int segmentSize = DEFAULT_SEGMENT_SIZE;
    private int maxSegmentCount = Integer.MAX_VALUE;
    private String type = Object.class.getName();
    private long retentionMillis = Long.MAX_VALUE;
    private long tenuringAgeMillis = Long.MAX_VALUE;
    private int backupCount = DEFAULT_SYNC_BACKUP_COUNT;
    private int asyncBackupCount = DEFAULT_ASYNC_BACKUP_COUNT;

    public LogConfig() {
    }

    public LogConfig(String name) {
        this.name = name;
    }

    public LogConfig(LogConfig config) {
        this.name = config.name;
        this.encoder = config.encoder;
        this.segmentSize = config.segmentSize;
        this.maxSegmentCount = config.maxSegmentCount;
        this.type = config.type;
        this.retentionMillis = config.retentionMillis;
        this.tenuringAgeMillis = config.tenuringAgeMillis;
    }

    /**
     * Get the total number of backups: the backup count plus the asynchronous backup count.
     *
     * @return the total number of backups
     */
    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    /**
     * Get the number of synchronous backups for this queue.
     *
     * @return the synchronous backup count
     */
    public int getBackupCount() {
        return backupCount;
    }

    /**
     * Sets the number of synchronous backups for this log.
     *
     * @param backupCount the number of synchronous backups to set
     * @return the current LogConfig
     * @throws IllegalArgumentException if backupCount is smaller than 0,
     *                                  or larger than the maximum number of backups,
     *                                  or the sum of the backups and async backups is larger than the maximum number of backups
     * @see #setAsyncBackupCount(int)
     */
    public LogConfig setBackupCount(int backupCount) {
        this.backupCount = checkBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    /**
     * Get the number of asynchronous backups for this log.
     *
     * @return the number of asynchronous backups
     */
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    /**
     * Sets the number of asynchronous backups. 0 means no backups.
     *
     * @param asyncBackupCount the number of asynchronous synchronous backups to set
     * @return the updated LogConfig
     * @throws IllegalArgumentException if asyncBackupCount smaller than 0,
     *                                  or larger than the maximum number of backup
     *                                  or the sum of the backups and async backups is larger than the maximum number of backups
     * @see #setBackupCount(int)
     * @see #getAsyncBackupCount()
     */
    public LogConfig setAsyncBackupCount(int asyncBackupCount) {
        this.asyncBackupCount = checkAsyncBackupCount(backupCount, asyncBackupCount);
        return this;
    }

    public long getTenuringAgeMillis() {
        return tenuringAgeMillis;
    }

    public LogConfig setTenuringAge(long age, TimeUnit unit) {
        return setTenuringAgeMillis(unit.toMillis(age));
    }

    public LogConfig setTenuringAgeMillis(long tenuringAgeMillis) {
        this.tenuringAgeMillis = checkPositive(tenuringAgeMillis, "tenuringAgeMillis should be larger than 0");
        return this;
    }

    public long getRetentionMillis() {
        return retentionMillis;
    }

    public LogConfig setRetentionMillis(long retentionPeriod, TimeUnit unit) {
        return setRetentionMillis(unit.toMillis(retentionPeriod));
    }

    public LogConfig setRetentionMillis(long retentionMillis) {
        this.retentionMillis = checkPositive(retentionMillis, "retentionMillis should be larger than 0");
        return this;
    }

    public String getType() {
        return type;
    }

    public LogConfig setType(Class clazz) {
        return setType(clazz.getName());
    }

    public LogConfig setType(String type) {
        this.type = checkNotNull(type);

        if (Long.TYPE.getName().equals(type)) {
            encoder = LongEncoder.INSTANCE;
        } else if (Integer.TYPE.getName().equals(type)) {
            encoder = IntEncoder.INSTANCE;
        } else if (Double.TYPE.getName().equals(type)) {
            encoder = DoubleEncoder.INSTANCE;
        }

        return this;
    }

    public int getSegmentSize() {
        return segmentSize;
    }

    public LogConfig setSegmentSize(int segmentSize) {
        this.segmentSize = segmentSize;
        return this;
    }

    public int getMaxSegmentCount() {
        return maxSegmentCount;
    }

    public LogConfig setMaxSegmentCount(int maxSegmentCount) {
        this.maxSegmentCount = maxSegmentCount;
        return this;
    }

    public LogConfig setEncoder(String encoderClass) {
        try {
            Class clazz = LogConfig.class.getClassLoader().loadClass(encoderClass);
            Encoder encoder = (Encoder) clazz.newInstance();
            setEncoder(encoder);
            return this;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public LogConfig setEncoder(Encoder encoder) {
        this.encoder = checkNotNull(encoder);
        return this;
    }

    public Encoder getEncoder() {
        return encoder;
    }

    @Override
    public LogConfig setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.LOG_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(backupCount);
        out.writeInt(asyncBackupCount);
        out.writeObject(encoder);
        out.writeInt(segmentSize);
        out.writeInt(maxSegmentCount);
        out.writeUTF(type);
        out.writeLong(retentionMillis);
        out.writeLong(tenuringAgeMillis);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.name = in.readUTF();
        this.backupCount = in.readInt();
        this.asyncBackupCount = in.readInt();
        this.encoder = in.readObject();
        this.segmentSize = in.readInt();
        this.maxSegmentCount = in.readInt();
        this.type = in.readUTF();
        this.retentionMillis = in.readLong();
        this.tenuringAgeMillis = in.readLong();
    }

    @Override
    public String toString() {
        return "LogConfig{" +
                "name='" + name + '\'' +
                ", backupCount=" + backupCount +
                ", asyncBackupCount=" + asyncBackupCount +
                ", encoder=" + encoder +
                ", segmentSize=" + segmentSize +
                ", maxSegmentCount=" + maxSegmentCount +
                ", retentionMillis=" + retentionMillis +
                ", tenuringAgeMillis=" + tenuringAgeMillis +
                '}';
    }


}
