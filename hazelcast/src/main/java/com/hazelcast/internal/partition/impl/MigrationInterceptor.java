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

package com.hazelcast.internal.partition.impl;


import com.hazelcast.internal.partition.MigrationInfo;

import java.util.Collection;
import java.util.EventListener;

/**
 * Internal synchronous/blocking listener to intercept migration cycle
 * on master member and migration participants.
 * <p>
 * It's used to execute a specific test scenario deterministically. There's no user side api or configuration.
 *
 * @see InternalPartitionServiceImpl#setMigrationInterceptor(MigrationInterceptor)
 */
public interface MigrationInterceptor extends EventListener {

    enum MigrationParticipant {
        MASTER,
        SOURCE,
        DESTINATION
    }

    default void onMigrationStart(MigrationParticipant participant, MigrationInfo migration) {
    }

    default void onMigrationComplete(MigrationParticipant participant, MigrationInfo migration, boolean success) {
    }

    default void onMigrationCommit(MigrationParticipant participant, MigrationInfo migration) {
    }

    default void onMigrationRollback(MigrationParticipant participant, MigrationInfo migration) {
    }

    default void onPromotionStart(MigrationParticipant participant, Collection<MigrationInfo> migrations) {
    }

    default void onPromotionComplete(MigrationParticipant participant, Collection<MigrationInfo> migrations, boolean success) {
    }

    class NopMigrationInterceptor implements MigrationInterceptor {
    }
}
