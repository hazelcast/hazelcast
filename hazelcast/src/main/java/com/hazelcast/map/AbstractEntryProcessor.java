/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.nio.serialization.SerializableByConvention;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;

import java.util.Map;

import static com.hazelcast.nio.serialization.SerializableByConvention.Reason.PUBLIC_API;

/**
 * An abstract {@link EntryProcessor} that already has implemented the {@link #getBackupProcessor()}. In a most cases you
 * want the same logic to be executed on the primary and on the backup. This implementation has this behavior.
 * <p/>
 * Note that there is a possibility which an {@link com.hazelcast.map.AbstractEntryProcessor} can see that a key exists
 * but its backup processor may not find it due to an unsent backup of a previous operation (e.g. a previous put).
 * In those situations, Hazelcast internally/eventually will sync those owner and backup partitions so you will not lose any data.
 * Because AbstractEntryProcessor uses the same processor in both owner and backup,
 * you should take this case into account when implementing {@link com.hazelcast.map.EntryProcessor#process(java.util.Map.Entry)}.
 *
 * @param <K> Type of key of a {@link java.util.Map.Entry}
 * @param <V> Type of value of a {@link java.util.Map.Entry}
 * @see com.hazelcast.map.EntryProcessor
 * @see com.hazelcast.map.EntryBackupProcessor
 */
public abstract class AbstractEntryProcessor<K, V> implements EntryProcessor<K, V> {

    private final EntryBackupProcessor<K, V> entryBackupProcessor;

    /**
     * Creates an AbstractEntryProcessor that applies the {@link #process(java.util.Map.Entry)} to primary and backups.
     */
    public AbstractEntryProcessor() {
        this(true);
    }

    /**
     * Creates an AbstractEntryProcessor.
     *
     * @param applyOnBackup true if the {@link #process(java.util.Map.Entry)} should also be applied on the backup.
     */
    public AbstractEntryProcessor(boolean applyOnBackup) {
        if (applyOnBackup) {
            entryBackupProcessor = new EntryBackupProcessorImpl();
        } else {
            entryBackupProcessor = null;
        }
    }

    @Override
    public final EntryBackupProcessor<K, V> getBackupProcessor() {
        return entryBackupProcessor;
    }

    @SerializableByConvention(PUBLIC_API)
    private class EntryBackupProcessorImpl implements EntryBackupProcessor<K, V>, HazelcastInstanceAware {
        // to fix https://github.com/hazelcast/hazelcast/issues/10083 we need to add the HazelcastInstanceAware
        // interface on the EntryBackupProcessorImpl. Unfortunately this changes the generated serialVersionUID
        // so sending this to a member which doesn't contain the fix would cause a deserialization exception.
        // That is why we will set the serialVersionUID here to the same generated value for a
        // EntryBackupProcessorImpl without the implemented interface. The value is calculated by a specification
        // based on types and fields so it should be the same on all JVM implementations
        static final long serialVersionUID = -5081502753526394129L;

        @Override
        public void processBackup(Map.Entry<K, V> entry) {
            process(entry);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            final AbstractEntryProcessor<K, V> outer = AbstractEntryProcessor.this;
            if (outer instanceof HazelcastInstanceAware) {
                ((HazelcastInstanceAware) outer).setHazelcastInstance(hazelcastInstance);
            }
        }
    }
}
