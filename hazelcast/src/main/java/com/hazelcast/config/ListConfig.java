/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.collection.IList;
import com.hazelcast.internal.config.ConfigDataSerializerHook;

/**
 * Contains the configuration for an {@link IList}.
 */
public class ListConfig extends CollectionConfig<ListConfig> {

    public ListConfig() {
    }

    public ListConfig(String name) {
        setName(name);
    }

    public ListConfig(ListConfig config) {
        super(config);
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.LIST_CONFIG;
    }

    @Override
    public String toString() {
        return "ListConfig{"
                + super.fieldsToString()
                + "}";
    }
}
