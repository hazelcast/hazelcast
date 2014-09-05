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

package com.hazelcast.concurrent.atomiclong.client;

import com.hazelcast.concurrent.atomiclong.operations.GetAndAddOperation;
import com.hazelcast.concurrent.idgen.IdGeneratorService;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.AtomicLongPermission;
import com.hazelcast.spi.Operation;

import java.security.Permission;

public class GetAndAddRequest extends AtomicLongRequest {

    public GetAndAddRequest() {
    }

    public GetAndAddRequest(String name, long delta) {
        super(name, delta);
    }

    @Override
    protected Operation prepareOperation() {
        return new GetAndAddOperation(name, delta);
    }

    @Override
    public int getClassId() {
        return AtomicLongPortableHook.GET_AND_ADD;
    }

    @Override
    public Permission getRequiredPermission() {
        if (name.startsWith(IdGeneratorService.ATOMIC_LONG_NAME)) {
            return null;
        }
        if (delta == 0) {
            return new AtomicLongPermission(name, ActionConstants.ACTION_READ);
        }
        return new AtomicLongPermission(name, ActionConstants.ACTION_MODIFY);
    }

    @Override
    public String getMethodName() {
        if (delta == 1) {
            return "getAndIncrement";
        } else if (delta == -1) {
            return "getAndDecrement";
        } else if (delta == 0) {
            return "get";
        }
        return "getAndAdd";
    }

    @Override
    public Object[] getParameters() {
        if (delta > 1 || delta < -1) {
            return super.getParameters();
        }
        return null;
    }
}
