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

package com.hazelcast.util.scheduler;

/**
 * Represents a composite key composed of an object and a long identifier. This object is used in case the
 * {@link SecondsBasedEntryTaskScheduler} is instantiated with {@link ScheduleType#FOR_EACH}. In this case, the scheduler
 * will be able to schedule the same key with different times, each of them with the same key but different {@code id}'s.
 */
final class CompositeKey {

    private final Object key;
    private final long id;

    public CompositeKey(Object key, long id) {
        this.key = key;
        this.id = id;
    }

    public Object getKey() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CompositeKey that = (CompositeKey) o;

        if (id != that.id) {
            return false;
        }
        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (int) (id ^ (id >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Key{" + "key=" + key + ", id=" + id + '}';
    }
}
