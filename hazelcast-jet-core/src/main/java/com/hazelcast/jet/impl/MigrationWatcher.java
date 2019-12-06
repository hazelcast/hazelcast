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

import com.hazelcast.cluster.Endpoint;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.partition.ReplicaMigrationEvent;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

public class MigrationWatcher {
    private final HazelcastInstance instance;
    private final UUID membershipListenerReg;
    private final UUID migrationListenerReg;
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

    private UUID registerMembershipListener(HazelcastInstance instance) {
        return instance.getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent event) {
                Endpoint endpoint = instance.getLocalEndpoint();
                if (endpoint != null && endpoint.getUuid() != null
                        && endpoint.getUuid().equals(event.getMember().getUuid())) {
                    // Ignore self. This listener is executed in an async way. When executing for the
                    // local member, jobs can be already running before we get to increment the counter.
                    // This solves the issue if there's just 1 member.
                    return;
                }
                changeCount.incrementAndGet();
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                changeCount.incrementAndGet();
            }
        });
    }

    private UUID registerMigrationListener(HazelcastInstance instance) {
        try {
            return instance.getPartitionService().addMigrationListener(new MigrationListener() {
                @Override
                public void migrationStarted(MigrationState state) {
                    changeCount.incrementAndGet();
                }

                @Override
                public void migrationFinished(MigrationState state) {
                    changeCount.incrementAndGet();
                }

                @Override
                public void replicaMigrationCompleted(ReplicaMigrationEvent event) {
                    changeCount.incrementAndGet();
                }

                @Override
                public void replicaMigrationFailed(ReplicaMigrationEvent event) {
                    changeCount.incrementAndGet();
                }
            });
        } catch (UnsupportedOperationException e) {
            // MigrationListener is not supported on client
            return null;
        }
    }
}
