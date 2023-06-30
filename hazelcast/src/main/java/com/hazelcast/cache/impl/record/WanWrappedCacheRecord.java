/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.record;

/**
 * Simple {@link CacheRecord} wrapper that includes information about whether this record should
 * be WAN replicated; namely used for {@link com.hazelcast.cache.impl.operation.CachePutAllBackupOperation}
 * as we have scenarios where individual keys have different WAN replication needs
 */
public class WanWrappedCacheRecord {
    private CacheRecord record;
    private boolean wanReplicated;

    public WanWrappedCacheRecord(CacheRecord record, boolean wanReplicated) {
        this.record = record;
        this.wanReplicated = wanReplicated;
    }

    public CacheRecord getRecord() {
        return record;
    }

    public boolean isWanReplicated() {
        return wanReplicated;
    }

    public void setWanReplicated(boolean wanReplicated) {
        this.wanReplicated = wanReplicated;
    }
}
