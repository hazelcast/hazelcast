/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.listener;

import com.hazelcast.map.EntryProcessor;

import java.util.Map;

/**
 * Utility class for {@link EntryProcessor}.
 */
public final class EntryProcessorUtil {

    private static final DirectBackupEntryProcessor DIRECT_BACKUP_ENTRY_PROCESSOR = new DirectBackupEntryProcessor();

    private static final class DirectBackupEntryProcessor<K, V, R> implements EntryProcessor<Object, Object, Object> {

        @Override
        public Object process(Map.Entry<Object, Object> entry) {
            return null;
        }

        private Object readResolve() {
            return DIRECT_BACKUP_ENTRY_PROCESSOR;
        }
    }

    private EntryProcessorUtil() {
    }

    /**
     * This method provides a special (singleton) backup {@link EntryProcessor}
     * that can be returned by {@link EntryProcessor#getBackupProcessor()}
     * to indicate that backups are to be made by transmitting all modifications
     * to the backup replicas instead of applying the backup entry processor there.
     * This may be useful in cases when loading the backup entries and/or applying
     * the backup entry processor is costly and direct copy of the modifications
     * is more efficient.
     * @return the direct backup processor singleton.
     */
    @SuppressWarnings("unchecked")
    public static <K, V, R> EntryProcessor<K, V, R> directBackupProcessor() {
        return (EntryProcessor<K, V, R>) DIRECT_BACKUP_ENTRY_PROCESSOR;
    }
}
