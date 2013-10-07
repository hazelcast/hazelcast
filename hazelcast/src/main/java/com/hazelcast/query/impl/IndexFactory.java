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
package com.hazelcast.query.impl;

import java.util.Properties;

/**
 * Factory to create custom index.
 */
public interface IndexFactory {
    /**
     * Create a custom index for a map value attribute.
     * <p/>
     * An implementation of that interface can be defined in the map configuration.
     * <code>
     * MapIndexFactoryConfig mapIndexFactoryConfig = new MapIndexFactoryConfig();
     * mapIndexFactoryConfig.setFactoryClassName(MyIndexFactory.class.getName());
     * <p/>
     * MapConfig mapConfig = new MapConfig("MyMap");
     * mapConfig.setMapIndexFactoryConfig(mapIndexFactoryConfig);
     * <p/>
     * </code>
     *
     * @param attribute  the attribute name.
     * @param ordered    is the index ordered.
     * @param properties the properties of the index.
     * @return an Index instance
     */
    Index createIndex(
            String attribute,
            boolean ordered,
            Properties properties);
}
