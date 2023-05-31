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

package com.hazelcast.htable.impl;

import com.hazelcast.internal.tpc.offheapmap.Bin;
import com.hazelcast.internal.tpc.offheapmap.Bout;
import com.hazelcast.internal.tpc.offheapmap.OffheapMap;
import com.hazelcast.internal.tpc.server.Cmd;

public final class HGetCmd extends Cmd {

    public static final byte ID  = 2;

    private final Bin key = new Bin();
    private final Bout value = new Bout();
    private HTableDataManager tableManager;

    public HGetCmd() {
        super(ID);
    }

    @Override
    public void init() {
        tableManager = managerRegistry.get(HTableDataManager.class);
    }

    @Override
    public void clear() {
        key.clear();
        value.clear();
    }

    @Override
    public int runit() throws Exception {
        OffheapMap map = tableManager.getOffheapMap(partitionId, null);

        key.init(request);

        value.init(response);
        map.get(key, value);
        return CMD_COMPLETED;
    }
}
