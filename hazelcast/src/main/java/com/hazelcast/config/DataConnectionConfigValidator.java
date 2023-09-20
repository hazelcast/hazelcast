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
package com.hazelcast.config;

import com.hazelcast.spi.annotation.PrivateApi;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates an instance of {@link DataConnectionConfig}.
 */
@PrivateApi
public final class DataConnectionConfigValidator {

    private DataConnectionConfigValidator() {
    }

    /**
     * Checks if this configuration object is in coherent state - has all required properties set.
     */
    @PrivateApi
    public static void validate(DataConnectionConfig conf) {
        List<String> errors = new ArrayList<>();
        if (conf.getName() == null || conf.getName().isEmpty()) {
            errors.add("Data connection name must be non-null and contain text");
        }
        if (conf.getType() == null || conf.getType().isEmpty()) {
            errors.add("Data connection type must be non-null and contain text");
        }
        if (conf.getProperties() == null) {
            errors.add("Data connection properties cannot be null, they can be empty");
        }
        if (!errors.isEmpty()) {
            throw new IllegalArgumentException(String.join(", ", errors));
        }
    }

}
