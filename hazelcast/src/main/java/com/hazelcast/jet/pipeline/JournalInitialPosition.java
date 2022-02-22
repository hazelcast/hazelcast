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

package com.hazelcast.jet.pipeline;

/**
 * When passed to an IMap/ICache Event Journal source, specifies which
 * event to start from. You can start from the oldest event still in the
 * journal or skip all the history and receive only the events that
 * occur after connecting to the event journal.
 * <p>
 * See:
 * <ul>
 *     <li>{@link Sources#mapJournal}
 *     <li>{@link Sources#remoteMapJournal}
 *     <li>{@link Sources#cacheJournal}
 *     <li>{@link Sources#remoteCacheJournal}
 * </ul>
 *
 * @since Jet 3.0
 */
public enum JournalInitialPosition {

    /**
     * Start from the oldest event still available.
     */
    START_FROM_OLDEST,

    /**
     * Skip all the history and emit only the events that occur after
     * connecting to the journal.
     */
    START_FROM_CURRENT
}
