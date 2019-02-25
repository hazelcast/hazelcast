/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

public class MigrationWatcher {
    private final HazelcastInstance instance;
    private final String membershipListenerReg;
    private final String migrationListenerReg;
    private final AtomicInteger changeCount = new AtomicInteger();

    public MigrationWatcher(HazelcastInstance instance) {
        this.instance = instance;
        migrationListenerReg = registerMigrationListener(instance);
        membershipListenerReg = registerMembershipListener(instance);
    }

    /**
     * Returns a {@link BooleanSupplier} that will tell if a migration took
     * place since the call to this method.
     */
    public BooleanSupplier createWatcher() {
        int startChangeCount = changeCount.get();
        return () -> changeCount.get() != startChangeCount;
    }

    public void deregister() {
        instance.getCluster().removeMembershipListener(membershipListenerReg);
        if (migrationListenerReg != null) {
            instance.getPartitionService().removeMigrationListener(migrationListenerReg);
        }
    }

    private String registerMembershipListener(HazelcastInstance instance) {
        return instance.getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                changeCount.incrementAndGet();
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                changeCount.incrementAndGet();
            }

            @Override
            public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
            }
        });
    }

    private String registerMigrationListener(HazelcastInstance instance) {
        try {
            return instance.getPartitionService().addMigrationListener(new MigrationListener() {
                @Override
                public void migrationStarted(MigrationEvent migrationEvent) {
                    changeCount.incrementAndGet();
                }

                @Override
                public void migrationCompleted(MigrationEvent migrationEvent) {
                    changeCount.incrementAndGet();
                }

                @Override
                public void migrationFailed(MigrationEvent migrationEvent) {
                    changeCount.incrementAndGet();
                }
            });
        } catch (UnsupportedOperationException e) {
            // MigrationListener is not supported on client
            return null;
        }
    }
}
