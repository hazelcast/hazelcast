/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.exec;

/**
 * Result of {@link Exec#advance()} call.
 */
public enum IterationResult {
    /** One or more rows are available, and more rows may follow on subsequent {@link Exec#advance()} calls. */
    FETCHED,

    /** Zero or more rows available and no more rows will be supplied. */
    FETCHED_DONE,

    /** More rows may arrive in future, but none are available at the moment. Executor should give up the thread. */
    WAIT
}
