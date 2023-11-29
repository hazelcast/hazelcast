/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.query.PagingPredicate;

import java.util.Comparator;
import java.util.Map.Entry;

public class AlwaysTruePagingPredicate<K, V> implements PagingPredicate<K, V> {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean apply(Entry<K, V> mapEntry) {
        return true;
    }

    @Override
    public void reset() {
    }

    @Override
    public void nextPage() {
    }

    @Override
    public void previousPage() {
    }

    @Override
    public int getPage() {
        return 1;
    }

    @Override
    public void setPage(int page) {
    }

    @Override
    public int getPageSize() {
        return 1;
    }

    @Override
    public Comparator<Entry<K, V>> getComparator() {
        return null;
    }

    @Override
    public Entry<K, V> getAnchor() {
        return null;
    }
}
