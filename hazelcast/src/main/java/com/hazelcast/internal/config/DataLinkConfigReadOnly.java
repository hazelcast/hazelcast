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

package com.hazelcast.internal.config;

import com.hazelcast.config.DataLinkConfig;

import javax.annotation.Nonnull;
import java.util.Properties;

public class DataLinkConfigReadOnly extends DataLinkConfig {

    public DataLinkConfigReadOnly(DataLinkConfig config) {
        super(config);
    }

    @Override
    public DataLinkConfig setName(String name) {
        throw readOnly();
    }

    private RuntimeException readOnly() {
        return new UnsupportedOperationException("Config '" + getName() + "' is read-only");
    }

    @Override
    public DataLinkConfig setType(@Nonnull String type) {
        throw readOnly();
    }

    @Override
    public DataLinkConfig setShared(boolean shared) {
        throw readOnly();
    }

    @Override
    public DataLinkConfig setProperties(Properties properties) {
        throw readOnly();
    }
}
