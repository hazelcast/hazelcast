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

package com.hazelcast.config.replacer;

import com.hazelcast.config.replacer.spi.ConfigReplacer;

import java.util.Properties;

/**
 * ConfigReplacer for replacing property names with property values for properties provided in {@link #init(Properties)} method.
 * The implementation can be used for replacing System properties.
 */
public class PropertyReplacer implements ConfigReplacer {

    private Properties properties;

    public void init(Properties properties) {
        this.properties = properties;
    }

    @Override
    public String getPrefix() {
        return "";
    }

    @Override
    public String getReplacement(String variable) {
        return properties.getProperty(variable);
    }

}
