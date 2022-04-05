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

package com.hazelcast.internal.util.filter;

/**
 * Filter matching only when both sub-filters are matching
 *
 * @param <T>
 */
public final class AndFilter<T> implements Filter<T> {

    private final Filter<T> filter1;
    private final Filter<T> filter2;

    public AndFilter(Filter<T> filter1, Filter<T> filter2) {
        this.filter1 = filter1;
        this.filter2 = filter2;
    }

    @Override
    public boolean accept(T object) {
        return filter1.accept(object) && filter2.accept(object);
    }
}
