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

    private final EntryBackupProcessor<K,V> entryBackupProcessor = new EntryBackupProcessorImpl();

    @Override
    public final EntryBackupProcessor<K, V> getBackupProcessor() {
         return entryBackupProcessor;
    }

    private class EntryBackupProcessorImpl implements EntryBackupProcessor<K,V>{
        @Override
        public void processBackup(Map.Entry<K, V> entry) {
            process(entry);
        }
    }
}
