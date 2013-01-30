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

package com.hazelcast.collection;

/**
 * @ali 1/23/13
 */
public class CollectionRecord {

    private long recordId = -1;

    private final Object object;

    public CollectionRecord(Object object) {
        this.object = object;
    }

    public CollectionRecord(long recordId, Object object) {
        this.recordId = recordId;
        this.object = object;
    }

    public long getRecordId() {
        return recordId;
    }

    public Object getObject() {
        return object;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CollectionRecord)) return false;

        CollectionRecord that = (CollectionRecord) o;
        if (recordId != -1 && that.recordId != -1) {
            if (recordId != that.recordId) return false;
        }
        if (!object.equals(that.object)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (recordId ^ (recordId >>> 32));
        result = 31 * result + object.hashCode();
        return result;
    }
}
