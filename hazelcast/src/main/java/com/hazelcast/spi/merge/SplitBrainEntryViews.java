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

package com.hazelcast.spi.merge;

import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.cardinality.impl.hyperloglog.HyperLogLog;
import com.hazelcast.collection.impl.collection.CollectionItem;
import com.hazelcast.collection.impl.queue.QueueItem;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapMergeContainer;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskDescriptor;
import com.hazelcast.spi.SplitBrainMergeEntryView;

import java.io.IOException;

/**
 * Provides static factory methods to create {@link SplitBrainMergeEntryView} instances.
 *
 * @since 3.10
 */
public final class SplitBrainEntryViews {

    private SplitBrainEntryViews() {
    }

    public static <K, V> SplitBrainMergeEntryView<K, V> createSplitBrainMergeEntryView(K key, V value) {
        return new SimpleSplitBrainEntryView<K, V>()
                .setKey(key)
                .setValue(value);
    }

    public static SplitBrainMergeEntryView<Long, Data> createSplitBrainMergeEntryView(CollectionItem item) {
        return new SimpleSplitBrainEntryView<Long, Data>()
                .setKey(item.getItemId())
                .setValue(item.getValue())
                .setCreationTime(item.getCreationTime());
    }

    public static SplitBrainMergeEntryView<Long, Data> createSplitBrainMergeEntryView(QueueItem item) {
        return new SimpleSplitBrainEntryView<Long, Data>()
                .setKey(item.getItemId())
                .setValue(item.getData())
                .setCreationTime(item.getCreationTime());
    }

    public static SplitBrainMergeEntryView<Data, Object> createSplitBrainMergeEntryView(MultiMapMergeContainer container,
                                                                                        MultiMapRecord record) {
        return new SimpleSplitBrainEntryView<Data, Object>()
                .setKey(container.getKey())
                .setValue(record.getObject())
                .setCreationTime(container.getCreationTime())
                .setLastAccessTime(container.getLastAccessTime())
                .setLastUpdateTime(container.getLastUpdateTime())
                .setHits(container.getHits());
    }

    public static SplitBrainMergeEntryView<Data, Object> createSplitBrainMergeEntryView(MultiMapContainer container, Data key,
                                                                                        MultiMapRecord record, int hits) {
        return new SimpleSplitBrainEntryView<Data, Object>()
                .setKey(key)
                .setValue(record.getObject())
                .setCreationTime(container.getCreationTime())
                .setLastAccessTime(container.getLastAccessTime())
                .setLastUpdateTime(container.getLastUpdateTime())
                .setHits(hits);
    }

    public static SplitBrainMergeEntryView<Data, Data> createSplitBrainMergeEntryView(Record record, Data dataValue) {
        return new SimpleSplitBrainEntryView<Data, Data>()
                .setKey(record.getKey())
                .setValue(dataValue)
                .setCreationTime(record.getCreationTime())
                .setExpirationTime(record.getExpirationTime())
                .setHits(record.getHits())
                .setLastAccessTime(record.getLastAccessTime())
                .setLastStoredTime(record.getLastStoredTime())
                .setLastUpdateTime(record.getLastUpdateTime())
                .setVersion(record.getVersion())
                .setTtl(record.getTtl());
    }

    public static SplitBrainMergeEntryView<Data, Object> createSplitBrainMergeEntryView(Record record) {
        return new SimpleSplitBrainEntryView<Data, Object>()
                .setKey(record.getKey())
                .setValue(record.getValue())
                .setCreationTime(record.getCreationTime())
                .setExpirationTime(record.getExpirationTime())
                .setHits(record.getHits())
                .setLastAccessTime(record.getLastAccessTime())
                .setLastStoredTime(record.getLastStoredTime())
                .setLastUpdateTime(record.getLastUpdateTime())
                .setVersion(record.getVersion())
                .setTtl(record.getTtl());
    }

    public static <R extends CacheRecord> SplitBrainMergeEntryView<Data, Data> createSplitBrainMergeEntryView(Data key,
                                                                                                              Data value,
                                                                                                              R record) {
        return new SimpleSplitBrainEntryView<Data, Data>()
                .setKey(key)
                .setValue(value)
                .setCreationTime(record.getCreationTime())
                .setExpirationTime(record.getExpirationTime())
                .setHits(record.getAccessHit())
                .setLastAccessTime(record.getLastAccessTime());
    }

    public static SplitBrainMergeEntryView<Object, Object> createSplitBrainMergeEntryView(ReplicatedRecord record) {
        return new SimpleSplitBrainEntryView<Object, Object>()
                .setKey(record.getKeyInternal())
                .setValue(record.getValueInternal())
                .setCreationTime(record.getCreationTime())
                .setHits(record.getHits())
                .setLastAccessTime(record.getLastAccessTime())
                .setLastUpdateTime(record.getUpdateTime())
                .setTtl(record.getTtlMillis());
    }

    public static SplitBrainMergeEntryView<String, HyperLogLog>
    createSplitBrainMergeEntryView(String name, HyperLogLog item) {
        return new SimpleSplitBrainEntryView<String, HyperLogLog>()
                .setKey(name)
                .setValue(item);
    }

    public static SplitBrainMergeEntryView<String, ScheduledTaskDescriptor> createSplitBrainMergeEntryView(
            ScheduledTaskDescriptor task) {
        return new SimpleSplitBrainEntryView<String, ScheduledTaskDescriptor>()
                .setKey(task.getDefinition().getName())
                .setValue(task);
    }

    /**
     * SimpleSplitBrainEntryView is a mutable implementation of {@link SplitBrainMergeEntryView}.
     *
     * @param <K> the type of key
     * @param <V> the type of value
     */
    @SuppressWarnings("checkstyle:methodcount")
    static class SimpleSplitBrainEntryView<K, V> implements SplitBrainMergeEntryView<K, V>, IdentifiedDataSerializable {

        private K key;
        private V value;
        private long cost = -1;
        private long creationTime = -1;
        private long expirationTime;
        private long hits = -1;
        private long lastAccessTime = -1;
        private long lastStoredTime = -1;
        private long lastUpdateTime = -1;
        private long version;
        private long ttl;

        SimpleSplitBrainEntryView() {
        }

        @Override
        public K getKey() {
            return key;
        }

        SimpleSplitBrainEntryView<K, V> setKey(K key) {
            this.key = key;
            return this;
        }

        @Override
        public V getValue() {
            return value;
        }

        SimpleSplitBrainEntryView<K, V> setValue(V value) {
            this.value = value;
            return this;
        }

        @Override
        public long getCost() {
            return cost;
        }

        SimpleSplitBrainEntryView<K, V> setCost(long cost) {
            this.cost = cost;
            return this;
        }

        @Override
        public long getCreationTime() {
            return creationTime;
        }

        SimpleSplitBrainEntryView<K, V> setCreationTime(long creationTime) {
            this.creationTime = creationTime;
            return this;
        }

        @Override
        public long getExpirationTime() {
            return expirationTime;
        }

        SimpleSplitBrainEntryView<K, V> setExpirationTime(long expirationTime) {
            this.expirationTime = expirationTime;
            return this;
        }

        @Override
        public long getHits() {
            return hits;
        }

        SimpleSplitBrainEntryView<K, V> setHits(long hits) {
            this.hits = hits;
            return this;
        }

        @Override
        public long getLastAccessTime() {
            return lastAccessTime;
        }

        SimpleSplitBrainEntryView<K, V> setLastAccessTime(long lastAccessTime) {
            this.lastAccessTime = lastAccessTime;
            return this;
        }

        @Override
        public long getLastStoredTime() {
            return lastStoredTime;
        }

        SimpleSplitBrainEntryView<K, V> setLastStoredTime(long lastStoredTime) {
            this.lastStoredTime = lastStoredTime;
            return this;
        }

        @Override
        public long getLastUpdateTime() {
            return lastUpdateTime;
        }

        SimpleSplitBrainEntryView<K, V> setLastUpdateTime(long lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
            return this;
        }

        @Override
        public long getVersion() {
            return version;
        }

        SimpleSplitBrainEntryView<K, V> setVersion(long version) {
            this.version = version;
            return this;
        }

        @Override
        public long getTtl() {
            return ttl;
        }

        SimpleSplitBrainEntryView<K, V> setTtl(long ttl) {
            this.ttl = ttl;
            return this;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            IOUtil.writeObject(out, key);
            IOUtil.writeObject(out, value);
            out.writeLong(cost);
            out.writeLong(creationTime);
            out.writeLong(expirationTime);
            out.writeLong(hits);
            out.writeLong(lastAccessTime);
            out.writeLong(lastStoredTime);
            out.writeLong(lastUpdateTime);
            out.writeLong(version);
            out.writeLong(ttl);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            key = IOUtil.readObject(in);
            value = IOUtil.readObject(in);
            cost = in.readLong();
            creationTime = in.readLong();
            expirationTime = in.readLong();
            hits = in.readLong();
            lastAccessTime = in.readLong();
            lastStoredTime = in.readLong();
            lastUpdateTime = in.readLong();
            version = in.readLong();
            ttl = in.readLong();
        }

        @Override
        public int getFactoryId() {
            return SplitBrainMergePolicyDataSerializerHook.F_ID;
        }

        @Override
        public int getId() {
            return SplitBrainMergePolicyDataSerializerHook.ENTRY_VIEW;
        }

        @Override
        @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SimpleSplitBrainEntryView<?, ?> that = (SimpleSplitBrainEntryView<?, ?>) o;
            if (cost != that.cost) {
                return false;
            }
            if (creationTime != that.creationTime) {
                return false;
            }
            if (expirationTime != that.expirationTime) {
                return false;
            }
            if (hits != that.hits) {
                return false;
            }
            if (lastAccessTime != that.lastAccessTime) {
                return false;
            }
            if (lastStoredTime != that.lastStoredTime) {
                return false;
            }
            if (lastUpdateTime != that.lastUpdateTime) {
                return false;
            }
            if (version != that.version) {
                return false;
            }
            if (ttl != that.ttl) {
                return false;
            }
            if (key != null ? !key.equals(that.key) : that.key != null) {
                return false;
            }
            return value != null ? value.equals(that.value) : that.value == null;
        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            result = 31 * result + (int) (cost ^ (cost >>> 32));
            result = 31 * result + (int) (creationTime ^ (creationTime >>> 32));
            result = 31 * result + (int) (expirationTime ^ (expirationTime >>> 32));
            result = 31 * result + (int) (hits ^ (hits >>> 32));
            result = 31 * result + (int) (lastAccessTime ^ (lastAccessTime >>> 32));
            result = 31 * result + (int) (lastStoredTime ^ (lastStoredTime >>> 32));
            result = 31 * result + (int) (lastUpdateTime ^ (lastUpdateTime >>> 32));
            result = 31 * result + (int) (version ^ (version >>> 32));
            result = 31 * result + (int) (ttl ^ (ttl >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "EntryView{"
                    + "key=" + key
                    + ", value=" + value
                    + ", cost=" + cost
                    + ", creationTime=" + creationTime
                    + ", expirationTime=" + expirationTime
                    + ", hits=" + hits
                    + ", lastAccessTime=" + lastAccessTime
                    + ", lastStoredTime=" + lastStoredTime
                    + ", lastUpdateTime=" + lastUpdateTime
                    + ", version=" + version
                    + ", ttl=" + ttl
                    + '}';
        }
    }
}
