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

import java.util.function.Function;
import java.io.Serializable;

/**
 * Event journal event projection expecting an integer event value and
 * projecting an increment by a specified delta.
 *
 * @param <EJ_TYPE> the type of the data-structure-specific event type
 */
class NewValueIncrementingFunction<EJ_TYPE> implements Function<EJ_TYPE, Integer>, Serializable {
    private final int delta;
    private final EventJournalEventAdapter<String, Integer, EJ_TYPE> journalEventAdapter;

    NewValueIncrementingFunction(int delta, EventJournalEventAdapter<String, Integer, EJ_TYPE> journalEventAdapter) {
        this.delta = delta;
        this.journalEventAdapter = journalEventAdapter;
    }

    @Override
    public Integer apply(EJ_TYPE input) {
        return journalEventAdapter.getNewValue(input) + delta;
    }
}
