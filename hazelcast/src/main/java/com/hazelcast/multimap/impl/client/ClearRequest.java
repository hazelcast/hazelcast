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

package com.hazelcast.multimap.impl.client;

import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.multimap.impl.MultiMapPortableHook;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.operations.MultiMapOperationFactory;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.spi.OperationFactory;

import java.security.Permission;
import java.util.Map;

public class ClearRequest extends MultiMapAllPartitionRequest implements RetryableRequest {

    public ClearRequest() {
    }

    public ClearRequest(String name) {
        super(name);
    }

    protected OperationFactory createOperationFactory() {
        return new MultiMapOperationFactory(name, MultiMapOperationFactory.OperationFactoryType.CLEAR);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        int totalAffectedEntries = 0;
        for (Object affectedEntries : map.values()) {
            totalAffectedEntries += (Integer) affectedEntries;
        }
        final MultiMapService service = getService();
        service.publishMultiMapEvent(name, EntryEventType.CLEAR_ALL, totalAffectedEntries);
        return null;
    }

    public int getClassId() {
        return MultiMapPortableHook.CLEAR;
    }

    public Permission getRequiredPermission() {
        return new MultiMapPermission(name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getMethodName() {
        return "clear";
    }
}
