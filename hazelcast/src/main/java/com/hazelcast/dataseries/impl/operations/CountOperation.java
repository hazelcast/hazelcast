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

package com.hazelcast.dataseries.impl.operations;

import static com.hazelcast.dataseries.impl.DataSeriesDataSerializerHook.COUNT_OPERATION;

public class CountOperation extends DataSeriesOperation {

    private transient long count;

    public CountOperation() {
    }

    public CountOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        count = partition.count();
    }

    @Override
    public Long getResponse() {
        return count;
    }

    @Override
    public int getId() {
        return COUNT_OPERATION;
    }
}
