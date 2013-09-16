/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap.txn;

import com.hazelcast.multimap.MultiMapDataSerializerHook;
import com.hazelcast.multimap.operations.MultiMapKeyBasedOperation;
import com.hazelcast.nio.serialization.Data;

/**
 * @author ali 4/12/13
 */
public class TxnGenerateRecordIdOperation extends MultiMapKeyBasedOperation implements TxnMultiMapOperation{

    public TxnGenerateRecordIdOperation() {
    }

    public TxnGenerateRecordIdOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    public void run() throws Exception {
        response = getOrCreateContainer().nextId();
    }

    public int getId() {
        return MultiMapDataSerializerHook.TXN_GENERATE_RECORD_ID;
    }
}
