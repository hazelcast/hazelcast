/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
 * This exception is thrown when an exception that is coming from server is not recognized by the protocol.
 * Class name of the original exception is included in the exception
 */
public class UndefinedErrorCodeException extends HazelcastException {

    private final String className;

    public UndefinedErrorCodeException(String message, String className) {
        super("Class name: " + className + ", Message: " + message);
        this.className = className;
    }

    /**
     * @return name of the original class name
     */
    public String getOriginClassName() {
        return className;
    }
}
