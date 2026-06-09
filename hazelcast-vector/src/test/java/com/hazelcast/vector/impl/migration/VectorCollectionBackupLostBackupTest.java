/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.migration;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.OperationPacketFilter;
import com.hazelcast.internal.server.PacketFilter;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.test.PacketFiltersUtil;

import static com.hazelcast.test.Accessors.getNode;

/**
 * Test anti-entropy in case of lost backup
 */
public class VectorCollectionBackupLostBackupTest extends VectorCollectionBackupTestBase {

    @Override
    protected <S, T> void runScenario(FunctionEx<HazelcastInstance, S> init,
                                      BiFunctionEx<HazelcastInstance, S, T> action,
                                      ConsumerEx<T> validation) {
        // 0. initialize
        // 1. break backups
        // 2. run action
        // 3. wait for anti-entropy to fix things
        // 4. terminate member
        // 5. wait for partition-related changes
        // 6. validate no data lost

        var toBeTerminated = members[1];
        var state1 = init.apply(toBeTerminated);

        PacketFiltersUtil.setCustomFilter(toBeTerminated,
                new BackupPacketDropFilter(getNode(toBeTerminated).getSerializationService(), 1.0f, PacketFilter.Action.DROP));

        var state2 = action.apply(toBeTerminated, state1);

        // note that waitAllForSafeState triggers anti-entropy checks
        waitAllForSafeState(members);

        toBeTerminated.getLifecycleService().terminate();
        waitAllForSafeState(members[0], members[2]);
        validation.accept(state2);
    }

    public static class BackupPacketDropFilter extends OperationPacketFilter implements PacketFilter {
        private final float blockRatio;
        private final Action action;

        BackupPacketDropFilter(InternalSerializationService serializationService, float blockRatio, Action action) {
            super(serializationService);
            this.blockRatio = blockRatio;
            this.action = action;
        }

        @Override
        protected Action filterOperation(Address endpoint, int factory, int type) {
            boolean isBackup = factory == SpiDataSerializerHook.F_ID && type == SpiDataSerializerHook.BACKUP;
            return !isBackup ? Action.ALLOW : (Math.random() > blockRatio ? Action.ALLOW : action);
        }
    }
}
