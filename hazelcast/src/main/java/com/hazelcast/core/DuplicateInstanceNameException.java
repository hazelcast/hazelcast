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

package com.hazelcast.core;

import com.hazelcast.nio.serialization.BinaryInterface;

import static com.hazelcast.nio.serialization.BinaryInterface.Reason.OTHER_CONVENTION;

/**
 * Thrown when a duplicate instance name is detected.
 */
@BinaryInterface(reason = OTHER_CONVENTION)
public class DuplicateInstanceNameException extends HazelcastException {

    /**
     * Returns the message when a duplicate instance name is detected.
     *
     * @param message the message when a duplicate instance name is detected
     */
    public DuplicateInstanceNameException(String message) {
        super(message);
    }
}
