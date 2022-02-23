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

package com.hazelcast.query.impl.getters;

import java.util.List;

/**
 * Immutable version of the MultiResult.
 *
 * @param <T> type of the underlying result store in the MultiResult
 */
public final class ImmutableMultiResult<T> extends MultiResult<T> {

    private final MultiResult<T> multiResult;

    public ImmutableMultiResult(MultiResult<T> multiResult) {
        this.multiResult = multiResult;
    }

    /**
     * @param result result to be added to this MultiResult
     */
    public void add(T result) {
        throw new UnsupportedOperationException("Can't modify an immutable MultiResult");
    }


    public void addNullOrEmptyTarget() {
        throw new UnsupportedOperationException("Can't modify an immutable MultiResult");
    }

    /**
     * @return a mutable underlying list of collected results
     */
    public List<T> getResults() {
        return multiResult.getResults();
    }

    /**
     * @return true if the MultiResult is empty; false otherwise
     */
    public boolean isEmpty() {
        return multiResult.isEmpty();
    }

    public boolean isNullEmptyTarget() {
        return multiResult.isNullEmptyTarget();
    }

    public void setNullOrEmptyTarget(boolean nullOrEmptyTarget) {
        throw new UnsupportedOperationException("Can't modify an immutable MultiResult");
    }
}
