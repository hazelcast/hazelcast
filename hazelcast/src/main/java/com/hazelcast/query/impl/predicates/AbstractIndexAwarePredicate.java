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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.QueryContext;

@BinaryInterface
public abstract class AbstractIndexAwarePredicate<K, V> extends AbstractPredicate<K, V> implements IndexAwarePredicate<K, V> {

    protected AbstractIndexAwarePredicate() {
    }

    protected AbstractIndexAwarePredicate(String attributeName) {
        super(attributeName);
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        return getIndex(queryContext) != null;
    }

    protected Index getIndex(QueryContext queryContext) {
        return queryContext.getIndex(attributeName);
    }

}
