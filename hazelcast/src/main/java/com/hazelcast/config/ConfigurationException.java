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

package com.hazelcast.config;

import com.hazelcast.core.HazelcastException;

import static java.lang.String.format;

/**
 * A {@link HazelcastException} that is thrown when something is wrong with the server or client configuration.
 */
public class ConfigurationException extends HazelcastException {

    public ConfigurationException(String itemName, String candidate, String duplicate) {
        super(format("Found ambiguous configurations for item \"%s\": \"%s\" vs. \"%s\"%nPlease specify your configuration.",
                itemName, candidate, duplicate));
    }

    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
