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

package com.hazelcast.cp.internal;

import com.hazelcast.cp.event.CPGroupAvailabilityEvent;

import java.util.EventListener;

/**
 * Unique registration key for each &lt;CPGroupAvailabilityEvent, CPGroupAvailabilityListener&gt; pair.
 * It's used to deduplicate multiple events with the same signature per listener.
 */
final class CPGroupAvailabilityEventKey {
    final CPGroupAvailabilityEvent event;
    final EventListener listener;

    CPGroupAvailabilityEventKey(CPGroupAvailabilityEvent event, EventListener listener) {
        this.event = event;
        this.listener = listener;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CPGroupAvailabilityEventKey that = (CPGroupAvailabilityEventKey) o;
        return listener.equals(that.listener) && event.equals(that.event);
    }

    @Override
    public int hashCode() {
        int result = event.hashCode();
        result = 31 * result + listener.hashCode();
        return result;
    }
}
