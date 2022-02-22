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

package com.hazelcast.config;

import com.hazelcast.internal.config.MultiMapConfigReadOnly;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultiMapConfigReadOnlyTest {

    private MultiMapConfig getReadOnlyConfig() {
        return new MultiMapConfigReadOnly(new MultiMapConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getEntryListenerConfigsOfReadOnlyMultiMapConfigShouldReturnUnmodifiable() {
        MultiMapConfig config = new MultiMapConfig()
                .addEntryListenerConfig(new EntryListenerConfig())
                .addEntryListenerConfig(new EntryListenerConfig());

        List<EntryListenerConfig> entryListenerConfigs = new MultiMapConfigReadOnly(config)
                .getEntryListenerConfigs();
        entryListenerConfigs.add(new EntryListenerConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setNameOfReadOnlyMultiMapConfigShouldFail() {
        getReadOnlyConfig().setName("my-multimap");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setValueCollectionTypeViaStringOfReadOnlyMultiMapConfigShouldFail() {
        getReadOnlyConfig().setValueCollectionType("SET");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setValueCollectionTypeOfReadOnlyMultiMapConfigShouldFail() {
        getReadOnlyConfig().setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addEntryListenerConfigOfReadOnlyMultiMapConfigShouldFail() {
        getReadOnlyConfig().addEntryListenerConfig(new EntryListenerConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setEntryListenerConfigsOfReadOnlyMultiMapConfigShouldFail() {
        getReadOnlyConfig().setEntryListenerConfigs(Collections.singletonList(new EntryListenerConfig()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setBinaryOfReadOnlyMultiMapConfigShouldFail() {
        getReadOnlyConfig().setBinary(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setBackupCountOfReadOnlyMultiMapConfigShouldFail() {
        getReadOnlyConfig().setBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setAsyncBackupCountOfReadOnlyMultiMapConfigShouldFail() {
        getReadOnlyConfig().setAsyncBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setStatisticsEnabledOfReadOnlyMultiMapConfigShouldFail() {
        getReadOnlyConfig().setStatisticsEnabled(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setMergePolicy() {
        getReadOnlyConfig().setMergePolicyConfig(new MergePolicyConfig());
    }
}
