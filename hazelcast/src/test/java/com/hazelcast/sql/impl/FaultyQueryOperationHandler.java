/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import com.hazelcast.sql.impl.operation.QueryOperation;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;

import java.util.UUID;

public final class FaultyQueryOperationHandler implements QueryOperationHandler {

    public static final FaultyQueryOperationHandler INSTANCE = new FaultyQueryOperationHandler();

    private FaultyQueryOperationHandler() {
        // No-op.
    }

    @Override
    public boolean submit(UUID sourceMemberId, UUID memberId, QueryOperation operation) {
        return false;
    }

    @Override
    public void execute(QueryOperation operation) {
        // No-op.
    }
}
