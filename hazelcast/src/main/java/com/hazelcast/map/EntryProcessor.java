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

package com.hazelcast.map;

import com.hazelcast.nio.serialization.BinaryInterface;

import java.io.Serializable;
import java.util.Map;

/**
 * An EntryProcessor passes you a {@link java.util.Map.Entry}. At the time you receive it
 * the entry is locked and not released until the EntryProcessor completes.
 * This obviates the need to explicitly lock as would be required with a {@link java.util.concurrent.ExecutorService}.
 * <p/>
 * Performance can be very high as the data is not moved off the Member partition. This avoids network cost and, if
 * the storage format is {@link com.hazelcast.config.InMemoryFormat#OBJECT}, then there is no de-serialization or serialization
 * cost.
 * <p/>
 * EntryProcessors execute on the partition thread in a member. Multiple operations on the same partition are queued.
 * <p/>
 * While executing partition migrations are not allowed. Any migrations are queued on the partition thread.
 * <p/>
 * An EntryProcessor may not be re-entrant i.e. it may not access the same {@link Map}. Limitation: you can only access
 * data on the same partition.
 * <p/>
 * Note that to modify an entry by using EntryProcessors you should explicitly call the
 * {@link java.util.Map.Entry#setValue} method of {@link java.util.Map.Entry} such as:
 * <p/>
 * <pre>
 * <code>
 * {@literal}Override
 *     public Object process(Map.Entry entry) {
 *        Value value = entry.getValue();
 *        // process and modify value
 *        // ...
 *        entry.setValue(value);
 *        return result;
 *    }
 * </code>
 * </pre>
 * otherwise EntryProcessor does not guarantee that it will modify the entry.
 *<p/>
 * EntryProcessor instances can be shared between threads. If an EntryProcessor instance contains mutable state, proper
 * concurrency control needs to be provided to coordinate access to mutable state. Another option is to rely on threadlocals.
 *
 * Serialized instances of this interface are used in client-member communication, so changing an implementation's binary format
 * will render it incompatible with its previous versions.
 *
 * @param <K> Type of key of a {@link java.util.Map.Entry}
 * @param <V> Type of value of a {@link java.util.Map.Entry}
 * @see AbstractEntryProcessor
 */
@BinaryInterface
public interface EntryProcessor<K, V> extends Serializable {

    /**
     * Process the entry without worrying about concurrency.
     * <p/>
     * Note that to modify an entry by using EntryProcessor you should explicitly call
     * {@link java.util.Map.Entry#setValue} method of {@link java.util.Map.Entry} such as:
     * <p/>
     * <pre>
     * <code>
     * {@literal @}Override
     *        public Object process(Map.Entry entry) {
     *          Value value = entry.getValue();
     *          // process and modify value
     *          // ...
     *          entry.setValue(value);
     *          return result;
     *        }
     * </code>
     * </pre>
     * otherwise the {@code EntryProcessor} does not guarantee to modify the entry.
     *
     * @param entry entry to be processed
     * @return a result that will be returned from the method taking the {@link EntryProcessor}, such as
     * {@link com.hazelcast.core.IMap#executeOnKey(Object, EntryProcessor) IMap.executeOnKey()}
     */
    Object process(Map.Entry<K, V> entry);

    /**
     * Get the entry processor to be applied to backup entries.
     *
     * In case of a readonly execution, {@code null} can be returned to indicate that no backups should be made.
     *
     * @return the back up processor
     */
    EntryBackupProcessor<K, V> getBackupProcessor();
}
