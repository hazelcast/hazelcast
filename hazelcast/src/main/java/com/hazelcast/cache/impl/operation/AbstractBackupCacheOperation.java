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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.nio.serialization.Data;

abstract class AbstractBackupCacheOperation extends AbstractCacheOperation {

    AbstractBackupCacheOperation(String name, Data key) {
        super(name, key);
    }

    AbstractBackupCacheOperation() {
    }


    protected abstract void runInternal() throws Exception;
    protected abstract void afterRunInternal() throws Exception;

    @Override
    public final void beforeRun() throws Exception {
        try {
            super.beforeRun();
        } catch (CacheNotExistsException e) {
            cache = null;
            getLogger().finest("Error while getting a cache", e);
        }
    }

    @Override
    public final void run() throws Exception {
        if (cache != null) {
            runInternal();
        }
    }

    @Override
    public final void afterRun() throws Exception {
        if (cache != null) {
            afterRunInternal();
        }
        super.afterRun();
    }
}
