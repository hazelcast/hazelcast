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

/**
 * A InvalidConfigurationException is thrown when there is an Invalid Configuration.
 * Invalid Configuration can be a wrong XML Config or logical config errors that are found
 * at real time.
 */
public class InvalidConfigurationException extends RuntimeException {

    /**
     * Creates a InvalidConfigurationException with the given message.
     *
     * @param message the message for the exception
     */
    public InvalidConfigurationException(String message) {
        super(message);
    }

    /**
     * Constructs a new runtime exception with the specified detail message and cause.
     *
     * @param message the message for the runtime exception
     * @param cause   the cause of the runtime exception
     */
    public InvalidConfigurationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
