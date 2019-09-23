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

package com.hazelcast.cp.internal.datastructures.unsafe;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.internal.datastructures.unsafe.atomiclong.AtomicLongContainer;
import com.hazelcast.cp.internal.datastructures.unsafe.atomiclong.AtomicLongService;
import com.hazelcast.test.HazelcastTestSupport;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.test.HazelcastTestSupport.getFirstBackupInstance;
import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static com.hazelcast.test.HazelcastTestSupport.getPartitionIdViaReflection;

public final class ConcurrencyTestUtil {

    private ConcurrencyTestUtil() {
    }

    /**
     * Returns the backup instance of an {@link IAtomicLong} by a given atomic long instance.
     * <p>
     * Note: Returns the backups from the first replica index.
     *
     * @param instances  the {@link HazelcastInstance} array to gather the data from
     * @param atomicLong the {@link IAtomicLong} to retrieve the backup from
     * @return the backup {@link AtomicLong}
     */
    public static AtomicLong getAtomicLongBackup(HazelcastInstance[] instances, IAtomicLong atomicLong) {
        int partitionId = getPartitionIdViaReflection(atomicLong);
        HazelcastInstance backupInstance = getFirstBackupInstance(instances, partitionId);
        return getAtomicLongBackup(backupInstance, atomicLong.getName());
    }

    /**
     * Returns the backup instance of an {@link IAtomicLong} by a given atomic long name.
     * <p>
     * Note: You have to provide the {@link HazelcastInstance} you want to retrieve the backups from.
     * Use {@link HazelcastTestSupport#getBackupInstance} to retrieve the backup instance for a given replica index.
     *
     * @param backupInstance the {@link HazelcastInstance} to retrieve the backup {@link AtomicLong} from
     * @param atomicLongName the atomic long name
     * @return the backup {@link AtomicLong}
     */
    public static AtomicLong getAtomicLongBackup(HazelcastInstance backupInstance, String atomicLongName) {
        AtomicLongService service = getNodeEngineImpl(backupInstance).getService(AtomicLongService.SERVICE_NAME);
        AtomicLongContainer container = service.getLongContainer(atomicLongName);
        return new AtomicLong(container.get());
    }
}
