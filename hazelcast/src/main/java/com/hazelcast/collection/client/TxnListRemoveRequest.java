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
import com.hazelcast.collection.list.ListService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.ListPermission;

import java.security.Permission;

public class TxnListRemoveRequest extends TxnCollectionRequest {

    public TxnListRemoveRequest() {
    }

    public TxnListRemoveRequest(String name, Data value) {
        super(name, value);
    }

    @Override
    public Object innerCall() throws Exception {
        return getEndpoint().getTransactionContext(txnId).getList(name).remove(value);
    }

    @Override
    public String getServiceName() {
        return ListService.SERVICE_NAME;
    }

    @Override
    public int getClassId() {
        return CollectionPortableHook.TXN_LIST_REMOVE;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ListPermission(name, ActionConstants.ACTION_REMOVE);
    }
}
