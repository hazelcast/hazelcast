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

package com.hazelcast.multimap.impl.txn;

import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.operations.AbstractKeyBasedMultiMapOperation;
import com.hazelcast.internal.serialization.Data;

public class TxnGenerateRecordIdOperation extends AbstractKeyBasedMultiMapOperation {

    public TxnGenerateRecordIdOperation() {
    }

    public TxnGenerateRecordIdOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    @Override
    public void run() throws Exception {
        response = getOrCreateContainer().nextId();
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.TXN_GENERATE_RECORD_ID;
    }
}
