/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

/**
 * Contains the configuration for an {@link IList}.
 */
public class ListConfig extends CollectionConfig<ListConfig> {

    private transient ListConfigReadOnly readOnly;

    public ListConfig() {
    }

    public ListConfig(String name) {
        setName(name);
    }

    public ListConfig(ListConfig config) {
        super(config);
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    @Override
    public ListConfig getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new ListConfigReadOnly(this);
        }
        return readOnly;
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
