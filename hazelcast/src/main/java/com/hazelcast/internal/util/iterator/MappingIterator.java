/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.iterator;

import com.hazelcast.core.IFunction;

import java.util.Iterator;

/**
 * Invoke operation on each member provided by the member iterator.
 *
 */
public class MappingIterator<I, O> implements Iterator<O> {
    private final Iterator<I> memberIterator;
    private final IFunction<I, O> mappingFunction;

    public MappingIterator(Iterator<I> memberIterator, IFunction<I, O> mappingFunction) {
        this.memberIterator = memberIterator;
        this.mappingFunction = mappingFunction;
    }

    @Override
    public boolean hasNext() {
        return memberIterator.hasNext();
    }

    @Override
    public O next() {
        I element = memberIterator.next();
        return mappingFunction.apply(element);
    }

    @Override
    public void remove() {
        memberIterator.remove();
    }
}
