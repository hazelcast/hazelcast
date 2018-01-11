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

package com.hazelcast.core;

/**
 * An unchecked version of {@link InterruptedException}.
 * <p>
 * Some of the Hazelcast operations may throw an <tt>RuntimeInterruptedException</tt>
 * if a user thread is interrupted while waiting a response.
 * Hazelcast uses RuntimeInterruptedException to pass InterruptedException up through interfaces
 * that don't have InterruptedException in their signatures. Users should be able to catch and handle
 * <tt>RuntimeInterruptedException</tt> in such cases as if their threads are interrupted on
 a blocking operation.
 * </p>
 *
 * @see InterruptedException
 */
public class RuntimeInterruptedException extends HazelcastException {

    public RuntimeInterruptedException() {
    }

    public RuntimeInterruptedException(String message) {
        super(message);
    }
}
