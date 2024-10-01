/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config.vector;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.vector.VectorCollectionConfig.MAX_BACKUP_COUNT;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class VectorCollectionConfigTest {

    @Test
    public void constructorNameValidation_failed() {
        assertThatThrownBy(() -> new VectorCollectionConfig("asd%6(&"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The name of the vector collection should "
                        + "only consist of letters, numbers, and the symbols \"-\", \"_\" or \"*\".");
    }

    @Test
    public void setNameValidation_failed() {
        assertThatThrownBy(() -> new VectorCollectionConfig().setName("asd*^"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The name of the vector collection should "
                        + "only consist of letters, numbers, and the symbols \"-\", \"_\" or \"*\".");
    }

    @Test
    public void constructorNameValidation_success() {
        assertThatNoException().isThrownBy(() -> new VectorCollectionConfig("***ve*c_tor-234-ANY_*"));
    }

    @Test
    public void setNameValidation_success() {
        assertThatNoException().isThrownBy(() -> new VectorCollectionConfig().setName("***ve*c_tor-234-ANY_*"));
    }

    @Test
    public void shouldValidateBackupCount() {
        assertThatNoException().isThrownBy(() ->
                new VectorCollectionConfig("a").setBackupCount(0).setAsyncBackupCount(0));
        assertThatNoException().isThrownBy(() ->
                new VectorCollectionConfig("a").setBackupCount(MAX_BACKUP_COUNT).setAsyncBackupCount(0));
        assertThatNoException().isThrownBy(() ->
                new VectorCollectionConfig("a").setBackupCount(0).setAsyncBackupCount(MAX_BACKUP_COUNT));

        assertThatThrownBy(() -> new VectorCollectionConfig("a").setBackupCount(MAX_BACKUP_COUNT).setAsyncBackupCount(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("the sum of backup-count and async-backup-count can't be larger than than 6");
        assertThatThrownBy(() -> new VectorCollectionConfig("a").setAsyncBackupCount(1).setBackupCount(MAX_BACKUP_COUNT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("the sum of backup-count and async-backup-count can't be larger than than 6");
    }
}
