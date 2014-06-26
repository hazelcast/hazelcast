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

package com.hazelcast.collection.client;

import com.hazelcast.collection.CollectionPortableHook;
import com.hazelcast.collection.set.SetService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.SetPermission;

import java.security.Permission;

public class TxnSetAddRequest extends TxnCollectionRequest {

    public TxnSetAddRequest() {
    }

    public TxnSetAddRequest(String name, Data value) {
        super(name, value);
    }

    @Override
    public Object innerCall() throws Exception {
        return getEndpoint().getTransactionContext(txnId).getSet(name).add(value);
    }

    @Override
    public String getServiceName() {
        return SetService.SERVICE_NAME;
    }

    @Override
    public int getClassId() {
        return CollectionPortableHook.TXN_SET_ADD;
    }

    @Override
    public Permission getRequiredPermission() {
        return new SetPermission(name, ActionConstants.ACTION_ADD);
    }
}
