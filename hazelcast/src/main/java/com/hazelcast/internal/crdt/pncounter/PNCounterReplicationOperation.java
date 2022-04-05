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

package com.hazelcast.internal.crdt.pncounter;

import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.internal.crdt.AbstractCRDTReplicationOperation;
import com.hazelcast.internal.crdt.CRDTDataSerializerHook;

import java.util.Map;

/**
 * CRDT replication operation for a {@link PNCounter}.
 *
 * @see AbstractCRDTReplicationOperation
 */
public class PNCounterReplicationOperation extends AbstractCRDTReplicationOperation<PNCounterImpl> {
    public PNCounterReplicationOperation() {
    }

    PNCounterReplicationOperation(Map<String, PNCounterImpl> migrationData) {
        super(migrationData);
    }

    @Override
    public int getClassId() {
        return CRDTDataSerializerHook.PN_COUNTER_REPLICATION;
    }

    @Override
    public String getServiceName() {
        return PNCounterService.SERVICE_NAME;
    }
}
