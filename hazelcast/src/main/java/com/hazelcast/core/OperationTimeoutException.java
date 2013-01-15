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

package com.hazelcast.core;

/**
 * An unchecked version of {@link java.util.concurrent.TimeoutException}.
 * <p>
 * Some of the Hazelcast operations may throw an <tt>OperationTimeoutException</tt>.
 * Hazelcast uses OperationTimeoutException to pass TimeoutException up through interfaces
 * that don't have TimeoutException in their signatures.
 * </p>
 *
 * @see java.util.concurrent.TimeoutException
 */
public class OperationTimeoutException extends HazelcastException {

    public OperationTimeoutException() {
    }

    public OperationTimeoutException(String message) {
        super(message);
    }

    public OperationTimeoutException(String op, String message) {
        super("[" + op + "] " + message);
    }
}
