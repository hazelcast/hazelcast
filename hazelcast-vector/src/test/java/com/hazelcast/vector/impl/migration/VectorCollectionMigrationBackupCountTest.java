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

import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * Tests basic backup and migration scenarios with different number of backups configured
 */
@Category({QuickTest.class, ParallelJVMTest.class})
public class VectorCollectionMigrationBackupCountTest extends VectorCollectionMigrationTest {

    @Parameterized.Parameter(2)
    public int backupCount;

    @Parameterized.Parameter(3)
    public int asyncBackupCount;

    @Parameterized.Parameters(name = "backupCount={2}, asyncBackupCount={3}")
    public static List<Object[]> parameters() {
        return cartesianProduct(List.of(false), List.of(false),
                List.of(0, 1, 2, VectorCollectionConfig.MAX_BACKUP_COUNT),
                List.of(0, 1, 2, VectorCollectionConfig.MAX_BACKUP_COUNT)
        ).stream().filter(p -> ((Integer) p[2]) + ((Integer) p[3]) <= VectorCollectionConfig.MAX_BACKUP_COUNT).toList();
    }

    @Override
    public int getBackupCount() {
        return backupCount;
    }

    @Override
    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }
}
