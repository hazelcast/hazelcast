/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.hibernate.region;

import com.hazelcast.core.HazelcastInstance;
import org.hibernate.cache.CacheDataDescription;
import org.hibernate.cache.TransactionalDataRegion;

import java.util.Properties;

/**
 * @author Leo Kim (lkim@limewire.com)
 */
public abstract class AbstractTransactionalDataRegion extends AbstractHazelcastRegion implements
        TransactionalDataRegion {

    private final CacheDataDescription metadata;

    protected AbstractTransactionalDataRegion(final HazelcastInstance instance, final String regionName,
                                              final Properties props, final CacheDataDescription metadata) {
        super(instance, regionName, props);
        this.metadata = metadata;
    }

    /**
     * @see org.hibernate.cache.TransactionalDataRegion#getCacheDataDescription()
     */
    public CacheDataDescription getCacheDataDescription() {
        return metadata;
    }

    /**
     * @see org.hibernate.cache.TransactionalDataRegion#isTransactionAware()
     */
    public boolean isTransactionAware() {
        return false;
    }
}
