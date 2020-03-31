/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.worker;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.sql.impl.operation.QueryOperation;

/**
 * Query operation to be executed. Could be either local (i.e. requested on the local member), or remote.
 */
public final class QueryOperationExecutable {
    private final QueryOperation localOperation;
    private final Packet remoteOperation;

    private QueryOperationExecutable(QueryOperation localOperation, Packet remoteOperation) {
        this.localOperation = localOperation;
        this.remoteOperation = remoteOperation;
    }

    public static QueryOperationExecutable local(QueryOperation localOperation) {
        return new QueryOperationExecutable(localOperation, null);
    }

    public static QueryOperationExecutable remote(Packet packet) {
        return new QueryOperationExecutable(null, packet);
    }

    public boolean isLocal() {
        return localOperation != null;
    }

    public QueryOperation getLocalOperation() {
        return localOperation;
    }

    public Packet getRemoteOperation() {
        return remoteOperation;
    }
}
