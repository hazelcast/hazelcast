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

package com.hazelcast.table.impl;

import com.hazelcast.internal.alto.offheapmap.ExampleQuery;
import com.hazelcast.internal.alto.offheapmap.OffheapMap;
import com.hazelcast.internal.alto.FrameCodec;
import com.hazelcast.internal.alto.Op;
import com.hazelcast.internal.alto.OpCodes;

public class QueryOp extends Op {

    // Currently, we always execute the same bogus query.
    // Probably the queryOp should have some query id for prepared queries
    // And we do a lookup based on that query id.
    // This query instance should also be pooled.
    private ExampleQuery query = new ExampleQuery();

    public QueryOp() {
        super(OpCodes.QUERY);
    }

    @Override
    public void clear() {
        query.clear();
    }

    @Override
    public int run() throws Exception {
        TableManager tableManager = managers.tableManager;
        OffheapMap map = tableManager.getOffheapMap(partitionId, null);

        map.execute(query);

        response.writeLong(query.result);
        return COMPLETED;
    }
}
