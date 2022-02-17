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

package com.hazelcast.spi.impl.securestore;

import com.hazelcast.core.HazelcastException;

/**
 * Exception class used to report run-time Secure Store related errors.
 */
public class SecureStoreException extends HazelcastException {
    /**
     * Creates a {@link SecureStoreException} with the given message.
     *
     * @param message the message for the exception
     */
    public SecureStoreException(String message) {
        super(message);
    }

    /**
     * Creates a {@link SecureStoreException} with the given detail message and cause.
     *
     * @param message the message for the exception
     * @param cause   the cause of the exception
     */
    public SecureStoreException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
