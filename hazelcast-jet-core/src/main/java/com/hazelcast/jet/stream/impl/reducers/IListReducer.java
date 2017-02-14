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

package com.hazelcast.jet.stream.impl.reducers;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.stream.IStreamList;

import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;

public class IListReducer<T> extends AbstractSinkReducer<T, IStreamList<T>> {

    private final String listName;

    public IListReducer() {
        this(uniqueListName());
    }

    private IListReducer(String listName) {
        this.listName = listName;
    }

    @Override
    protected IStreamList<T> getTarget(JetInstance instance) {
        return instance.getList(listName);
    }

    @Override
    protected ProcessorMetaSupplier getSupplier() {
        return ProcessorMetaSupplier.of(Processors.writeList(listName));
    }

    @Override
    protected int localParallelism() {
        return 1;
    }

    @Override
    protected String getName() {
        return listName;
    }

}
