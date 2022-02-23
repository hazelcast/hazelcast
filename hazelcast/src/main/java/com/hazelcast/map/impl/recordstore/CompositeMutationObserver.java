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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.record.Record;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.util.Collections.EMPTY_LIST;

class CompositeMutationObserver<R extends Record> implements MutationObserver<R> {

    private static final int DEFAULT_OBSERVER_COUNT = 4;

    private List<MutationObserver> mutationObservers = EMPTY_LIST;

    CompositeMutationObserver() {
    }

    public void add(MutationObserver<R> mutationObserver) {
        if (mutationObservers == EMPTY_LIST) {
            mutationObservers = new ArrayList<>(DEFAULT_OBSERVER_COUNT);
        }
        mutationObservers.add(mutationObserver);
    }

    @Override
    public void onClear() {
        Throwable throwable = null;
        for (int i = 0; i < mutationObservers.size(); i++) {
            try {
                mutationObservers.get(i).onClear();
            } catch (Throwable t) {
                if (throwable == null) {
                    throwable = t;
                }
            }
        }

        if (throwable != null) {
            throw rethrow(throwable);
        }
    }

    @Override
    public void onPutRecord(Data key, R record,
                            Object oldValue, boolean backup) {
        Throwable throwable = null;
        for (int i = 0; i < mutationObservers.size(); i++) {
            try {
                mutationObservers.get(i).onPutRecord(key, record, oldValue, backup);
            } catch (Throwable t) {
                if (throwable == null) {
                    throwable = t;
                }
            }
        }

        if (throwable != null) {
            throw rethrow(throwable);
        }
    }

    @Override
    public void onReplicationPutRecord(@Nonnull Data key,
                                       @Nonnull R record, boolean populateIndex) {
        Throwable throwable = null;
        for (int i = 0; i < mutationObservers.size(); i++) {
            try {
                mutationObservers.get(i).onReplicationPutRecord(key, record, populateIndex);
            } catch (Throwable t) {
                if (throwable == null) {
                    throwable = t;
                }
            }
        }

        if (throwable != null) {
            throw rethrow(throwable);
        }
    }

    @Override
    public void onUpdateRecord(@Nonnull Data key, @Nonnull R record,
                               Object oldValue, Object newValue, boolean backup) {
        Throwable throwable = null;
        for (int i = 0; i < mutationObservers.size(); i++) {
            try {
                mutationObservers.get(i).onUpdateRecord(key, record, oldValue, newValue, backup);
            } catch (Throwable t) {
                if (throwable == null) {
                    throwable = t;
                }
            }
        }

        if (throwable != null) {
            throw rethrow(throwable);
        }
    }

    @Override
    public void onRemoveRecord(Data key, R record) {
        Throwable throwable = null;
        for (int i = 0; i < mutationObservers.size(); i++) {
            try {
                mutationObservers.get(i).onRemoveRecord(key, record);
            } catch (Throwable t) {
                if (throwable == null) {
                    throwable = t;
                }
            }
        }

        if (throwable != null) {
            throw rethrow(throwable);
        }
    }

    @Override
    public void onEvictRecord(Data key, R record) {
        Throwable throwable = null;
        for (int i = 0; i < mutationObservers.size(); i++) {
            try {
                mutationObservers.get(i).onEvictRecord(key, record);
            } catch (Throwable t) {
                if (throwable == null) {
                    throwable = t;
                }
            }
        }

        if (throwable != null) {
            throw rethrow(throwable);
        }
    }

    @Override
    public void onLoadRecord(@Nonnull Data key, @Nonnull R record, boolean backup) {
        Throwable throwable = null;
        for (int i = 0; i < mutationObservers.size(); i++) {
            try {
                mutationObservers.get(i).onLoadRecord(key, record, backup);
            } catch (Throwable t) {
                if (throwable == null) {
                    throwable = t;
                }
            }
        }

        if (throwable != null) {
            throw rethrow(throwable);
        }
    }

    @Override
    public void onDestroy(boolean isDuringShutdown, boolean internal) {
        Throwable throwable = null;
        for (int i = 0; i < mutationObservers.size(); i++) {
            try {
                mutationObservers.get(i).onDestroy(isDuringShutdown, internal);
            } catch (Throwable t) {
                if (throwable == null) {
                    throwable = t;
                }
            }
        }

        if (throwable != null) {
            throw rethrow(throwable);
        }
    }

    @Override
    public void onReset() {
        Throwable throwable = null;
        for (int i = 0; i < mutationObservers.size(); i++) {
            try {
                mutationObservers.get(i).onReset();
            } catch (Throwable t) {
                if (throwable == null) {
                    throwable = t;
                }
            }
        }

        if (throwable != null) {
            throw rethrow(throwable);
        }
    }
}
