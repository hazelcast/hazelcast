/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Map;

/**
 * An abstract {@link EntryProcessor} that already has implemented the {@link #getBackupProcessor()}. In a most cases you
 * want the same logic to be executed on the primary and on the backup. This implementation has this behavior.
 *
 * @param <K>
 * @param <V>
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
     * @param applyOnBackup if the {@link #process(java.util.Map.Entry)} should also be applied on the backup.
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

    private class EntryBackupProcessorImpl implements EntryBackupProcessor<K, V> {
        @Override
        public void processBackup(Map.Entry<K, V> entry) {
            process(entry);
        }
    }
}
