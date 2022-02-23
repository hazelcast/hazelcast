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

package com.hazelcast.transaction.impl.xa.operations;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.SerializableList;
import com.hazelcast.transaction.impl.TransactionDataSerializerHook;
import com.hazelcast.transaction.impl.xa.SerializableXID;
import com.hazelcast.transaction.impl.xa.XAService;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CollectRemoteTransactionsOperation extends AbstractXAOperation {

    private transient SerializableList xidSet;

    public CollectRemoteTransactionsOperation() {
    }

    @Override
    public void run() throws Exception {
        XAService xaService = getService();
        NodeEngine nodeEngine = getNodeEngine();
        Set<SerializableXID> xids = xaService.getPreparedXids();
        List<Data> xidSet = new ArrayList<Data>();
        for (SerializableXID xid : xids) {
            xidSet.add(nodeEngine.toData(xid));
        }
        this.xidSet = new SerializableList(xidSet);
    }

    @Override
    public Object getResponse() {
        return xidSet;
    }

    @Override
    public int getClassId() {
        return TransactionDataSerializerHook.COLLECT_REMOTE_TX;
    }

}
