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

import com.hazelcast.internal.alto.offheapmap.Bin;
import com.hazelcast.internal.alto.offheapmap.OffheapMap;
import com.hazelcast.internal.alto.FrameCodec;
import com.hazelcast.internal.alto.Op;
import com.hazelcast.internal.alto.OpCodes;

public final class SetOp extends Op {

    private final Bin key = new Bin();
    private final Bin value = new Bin();

    public SetOp() {
        super(OpCodes.SET);
    }

    @Override
    public void clear() {
        key.clear();
        value.clear();
    }

    @Override
    public int run() throws Exception {
        TableManager tableManager = managers.tableManager;
        OffheapMap map = tableManager.getOffheapMap(partitionId, null);

        key.init(request);
        value.init(request);

        map.set(key, value);

        return COMPLETED;
    }
}
