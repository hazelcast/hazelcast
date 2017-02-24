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

package com.hazelcast.map.impl.operation;

import com.hazelcast.core.HazelcastException;

/**
 * Actor of the EntryOffloadableOperation -> EntryOffloadableSetUnlockOperation combined operations flow.
 * If returned from the EntryOffloadableSetUnlockOperation, the preceding EntryOffloadableOperation will be retried.
 */
public class EntryOffloadableLockMismatchException extends HazelcastException {

    public EntryOffloadableLockMismatchException() {
    }

    public EntryOffloadableLockMismatchException(final String message) {
        super(message);
    }

    public EntryOffloadableLockMismatchException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public EntryOffloadableLockMismatchException(final Throwable cause) {
        super(cause);
    }

}
