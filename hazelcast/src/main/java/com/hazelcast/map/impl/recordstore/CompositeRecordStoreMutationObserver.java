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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.map.impl.record.Record;

import java.util.Collection;
import java.util.LinkedList;

class CompositeRecordStoreMutationObserver<R extends Record> implements RecordStoreMutationObserver<R> {

    private final Collection<RecordStoreMutationObserver<R>> mutationObservers = new
            LinkedList<RecordStoreMutationObserver<R>>();

    CompositeRecordStoreMutationObserver(Collection<RecordStoreMutationObserver<R>> mutationObservers) {
        this.mutationObservers.addAll(mutationObservers);
    }

    @Override
    public void onClear() {
        for (RecordStoreMutationObserver<R> mutationObserver : mutationObservers) {
            mutationObserver.onClear();
        }
    }

    @Override
    public void onPutRecord(R record) {
        for (RecordStoreMutationObserver<R> mutationObserver : mutationObservers) {
            mutationObserver.onPutRecord(record);
        }
    }

    @Override
    public void onReplicationPutRecord(R record) {
        for (RecordStoreMutationObserver<R> mutationObserver : mutationObservers) {
            mutationObserver.onReplicationPutRecord(record);
        }
    }

    @Override
    public void onUpdateRecord(R record, Object newValue) {
        for (RecordStoreMutationObserver<R> mutationObserver : mutationObservers) {
            mutationObserver.onUpdateRecord(record, newValue);
        }
    }

    @Override
    public void onRemoveRecord(R record) {
        for (RecordStoreMutationObserver<R> mutationObserver : mutationObservers) {
            mutationObserver.onRemoveRecord(record);
        }
    }

    @Override
    public void onEvictRecord(R record) {
        for (RecordStoreMutationObserver<R> mutationObserver : mutationObservers) {
            mutationObserver.onEvictRecord(record);
        }
    }

    @Override
    public void onLoadRecord(R record) {
        for (RecordStoreMutationObserver<R> mutationObserver : mutationObservers) {
            mutationObserver.onLoadRecord(record);
        }
    }

    @Override
    public void onDestroy(boolean internal) {
        for (RecordStoreMutationObserver<R> mutationObserver : mutationObservers) {
            mutationObserver.onDestroy(internal);
        }
    }

    @Override
    public void onReset() {
        for (RecordStoreMutationObserver<R> mutationObserver : mutationObservers) {
            mutationObserver.onReset();
        }
    }
}
