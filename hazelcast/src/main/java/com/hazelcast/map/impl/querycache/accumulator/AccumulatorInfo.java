/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl.querycache.accumulator;

import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.map.impl.client.MapPortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Metadata for an {@link Accumulator}.
 * <p>
 * This metadata is used in communications between: node <--> node and client <--> node.
 *
 * @see QueryCacheConfig
 */
public class AccumulatorInfo implements Portable {

    private String mapName;
    private String cacheId;
    private Predicate predicate;
    private int batchSize;
    private int bufferSize;
    private long delaySeconds;
    private boolean includeValue;
    private boolean populate;
    private boolean coalesce;

    /**
     * Used to enable/disable {@link Accumulator} event sending functionality.
     * <p>
     * Used in the phase of initial population for preventing any event to be sent before taking the snapshot of {@code IMap}.
     */
    private volatile boolean publishable;

    public static AccumulatorInfo createAccumulatorInfo(QueryCacheConfig config, String mapName, String cacheId,
                                                        Predicate predicate) {
        checkNotNull(config, "config cannot be null");

        AccumulatorInfo info = new AccumulatorInfo();
        info.mapName = mapName;
        info.cacheId = cacheId;
        info.batchSize = calculateBatchSize(config);
        info.bufferSize = config.getBufferSize();
        info.delaySeconds = config.getDelaySeconds();
        info.includeValue = config.isIncludeValue();
        info.populate = config.isPopulate();
        info.predicate = getPredicate(config, predicate);
        info.coalesce = config.isCoalesce();
        info.publishable = false;
        return info;
    }

    @SuppressWarnings("checkstyle:parameternumber")
    public static AccumulatorInfo createAccumulatorInfo(String mapName, String cacheId, Predicate predicate, int batchSize,
                                                        int bufferSize, long delaySeconds, boolean includeValue, boolean populate,
                                                        boolean coalesce) {
        AccumulatorInfo info = new AccumulatorInfo();
        info.mapName = mapName;
        info.cacheId = cacheId;
        info.batchSize = batchSize;
        info.bufferSize = bufferSize;
        info.delaySeconds = delaySeconds;
        info.includeValue = includeValue;
        info.populate = populate;
        info.predicate = predicate;
        info.coalesce = coalesce;
        info.publishable = false;
        return info;
    }

    private static Predicate getPredicate(QueryCacheConfig config, Predicate predicate) {
        if (predicate != null) {
            return predicate;
        }

        Predicate implementation = config.getPredicateConfig().getImplementation();
        if (implementation != null) {
            return implementation;
        }

        throw new IllegalArgumentException("Predicate cannot be null");
    }

    private static int calculateBatchSize(QueryCacheConfig config) {
        // batchSize can not be higher than bufferSize.
        int batchSize = config.getBatchSize();
        int bufferSize = config.getBufferSize();
        return batchSize > bufferSize ? bufferSize : batchSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public long getDelaySeconds() {
        return delaySeconds;
    }

    public boolean isIncludeValue() {
        return includeValue;
    }

    public String getMapName() {
        return mapName;
    }

    public String getCacheId() {
        return cacheId;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public boolean isPublishable() {
        return publishable;
    }

    public boolean isPopulate() {
        return populate;
    }

    public void setPublishable(boolean publishable) {
        this.publishable = publishable;
    }

    public boolean isCoalesce() {
        return coalesce;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapPortableHook.CREATE_ACCUMULATOR_INFO;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("mn", mapName);
        writer.writeUTF("cn", cacheId);
        writer.writeInt("bas", batchSize);
        writer.writeInt("bus", bufferSize);
        writer.writeLong("ds", delaySeconds);
        writer.writeBoolean("iv", includeValue);
        writer.writeBoolean("ps", publishable);
        writer.writeBoolean("co", coalesce);
        writer.writeBoolean("po", populate);
        ObjectDataOutput output = writer.getRawDataOutput();
        output.writeObject(predicate);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        mapName = reader.readUTF("mn");
        cacheId = reader.readUTF("cn");
        batchSize = reader.readInt("bas");
        bufferSize = reader.readInt("bus");
        delaySeconds = reader.readLong("ds");
        includeValue = reader.readBoolean("iv");
        publishable = reader.readBoolean("ps");
        coalesce = reader.readBoolean("co");
        populate = reader.readBoolean("po");
        ObjectDataInput input = reader.getRawDataInput();
        predicate = input.readObject();
    }

    @Override
    public String toString() {
        return "AccumulatorInfo{"
                + "batchSize=" + batchSize
                + ", mapName='" + mapName + '\''
                + ", cacheId='" + cacheId + '\''
                + ", predicate=" + predicate
                + ", bufferSize=" + bufferSize
                + ", delaySeconds=" + delaySeconds
                + ", includeValue=" + includeValue
                + ", populate=" + populate
                + ", coalesce=" + coalesce
                + ", publishable=" + publishable
                + '}';
    }
}
