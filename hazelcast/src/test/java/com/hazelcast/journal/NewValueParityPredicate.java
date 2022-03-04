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

package com.hazelcast.journal;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * Event journal event predicate expecting an integer event value and
 * checking the parity of the new value.
 *
 * @param <EJ_TYPE> the type of the data-structure-specific event type
 */
class NewValueParityPredicate<EJ_TYPE> implements Predicate<EJ_TYPE>, Serializable {
    private final int remainder;
    private final EventJournalEventAdapter<String, Integer, EJ_TYPE> journalEventAdapter;

    NewValueParityPredicate(int remainder, EventJournalEventAdapter<String, Integer, EJ_TYPE> journalEventAdapter) {
        this.remainder = remainder;
        this.journalEventAdapter = journalEventAdapter;
    }

    @Override
    public boolean test(EJ_TYPE e) {
        return journalEventAdapter.getNewValue(e) % 2 == remainder;
    }
}
