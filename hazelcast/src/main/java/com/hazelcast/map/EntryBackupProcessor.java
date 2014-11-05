/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

import java.io.Serializable;
import java.util.Map;

/**
 * Processes an entry on a backup partition.
 * <p/>
 * Note that there is a possibility which an {@link com.hazelcast.map.EntryProcessor} can see
 * that a key exists but its backup processor {@link com.hazelcast.map.EntryBackupProcessor} may not find it
 * at the time of running due to an unsent backup of a previous operation (e.g. a previous put). In those situations,
 * Hazelcast internally/eventually will sync those owner and backup partitions so you will not lose any data.
 * But when coding an {@link com.hazelcast.map.EntryBackupProcessor}, one should take that case into account otherwise
 * {@link java.lang.NullPointerException}s can be seen since {@link java.util.Map.Entry#getValue()} may return null.
 *
 * @param <K> Type of key of a {@link java.util.Map.Entry}
 * @param <V> Type of value of a {@link java.util.Map.Entry}
 * @see com.hazelcast.map.EntryProcessor
 * @see com.hazelcast.map.AbstractEntryProcessor
 */
public interface EntryBackupProcessor<K, V> extends Serializable {

    void processBackup(Map.Entry<K, V> entry);
}
