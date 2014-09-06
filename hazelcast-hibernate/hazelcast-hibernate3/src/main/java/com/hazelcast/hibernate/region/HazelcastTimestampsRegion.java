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

package com.hazelcast.hibernate.region;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.hibernate.RegionCache;
import org.hibernate.cache.TimestampsRegion;

import java.util.Properties;

/**
 * Hazelcast based timestamp using region implementation
 *
 * @param <Cache> implementation type of RegionCache
 */
public class HazelcastTimestampsRegion<Cache extends RegionCache>
        extends AbstractGeneralRegion<Cache> implements TimestampsRegion {

    public HazelcastTimestampsRegion(final HazelcastInstance instance, final String name,
                                     final Properties props, final Cache cache) {
        super(instance, name, props, cache);
    }

}
