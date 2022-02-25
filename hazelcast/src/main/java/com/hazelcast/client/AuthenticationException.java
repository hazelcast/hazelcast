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

package com.hazelcast.client;

import com.hazelcast.core.HazelcastException;

/**
 * A {@link HazelcastException} that is thrown when there is an Authentication failure: e.g. credentials from client is not valid.
 */
public class AuthenticationException extends HazelcastException {

    /**
     * Creates a AuthenticationException with a default message.
     */
    public AuthenticationException() {
        super("Wrong cluster name or password.");
    }

    /**
     * Creates a AuthenticationException with the given message.
     *
     * @param message the message.
     */
    public AuthenticationException(String message) {
        super(message);
    }
}
