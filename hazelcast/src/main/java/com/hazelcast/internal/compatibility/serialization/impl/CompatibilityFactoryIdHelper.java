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

package com.hazelcast.internal.compatibility.serialization.impl;

import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.logging.Logger;

/**
 * Factory IDs for compatibility with compatibility (3.x) members
 */
public final class CompatibilityFactoryIdHelper {

    public static final String MAP_DS_FACTORY = "hazelcast.serialization.compatibility.ds.map";
    public static final int MAP_DS_FACTORY_ID = -10;

    public static final String CACHE_DS_FACTORY = "hazelcast.serialization.compatibility.ds.cache";
    public static final int CACHE_DS_FACTORY_ID = -25;

    public static final String ENTERPRISE_WAN_REPLICATION_DS_FACTORY
            = "hazelcast.serialization.compatibility.ds.enterprise.wan_replication";
    public static final int ENTERPRISE_WAN_REPLICATION_DS_FACTORY_ID = -28;

    public static final String WAN_REPLICATION_DS_FACTORY = "hazelcast.serialization.compatibility.ds.wan_replication";
    public static final int WAN_REPLICATION_DS_FACTORY_ID = -31;

    public static final String SPLIT_BRAIN_DS_FACTORY = "hazelcast.serialization.compatibility.ds.split_brain";
    public static final int SPLIT_BRAIN_DS_FACTORY_ID = -47;

    // factory ID 0 is reserved for Cluster objects (Data, Address, Member etc)...

    private CompatibilityFactoryIdHelper() {
    }

    public static int getFactoryId(String prop, int defaultId) {
        final String value = System.getProperty(prop);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                Logger.getLogger(FactoryIdHelper.class).finest("Parameter for property prop could not be parsed", e);
            }
        }
        return defaultId;
    }
}
