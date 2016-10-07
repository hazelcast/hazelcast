/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MultiMapConfigReadOnlyTest {

    private MultiMapConfigReadOnly getMultiMapConfigReadOnly() {
        return new MultiMapConfig().getAsReadOnly();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void gettingEntryListenerConfigsOfReadOnlyMultiMapConfigShouldReturnUnmodifiable() {
        MultiMapConfig config = new MultiMapConfig()
                .addEntryListenerConfig(new EntryListenerConfig())
                .addEntryListenerConfig(new EntryListenerConfig());

        List<EntryListenerConfig> entryListenerConfigs = config.getAsReadOnly().getEntryListenerConfigs();
        entryListenerConfigs.add(new EntryListenerConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingNameOfReadOnlyMultiMapConfigShouldFail() {
        getMultiMapConfigReadOnly().setName("my-multimap");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingValueCollectionTypeViaStringOfReadOnlyMultiMapConfigShouldFail() {
        getMultiMapConfigReadOnly().setValueCollectionType("SET");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingValueCollectionTypeOfReadOnlyMultiMapConfigShouldFail() {
        getMultiMapConfigReadOnly().setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void addingEntryListenerConfigOfReadOnlyMultiMapConfigShouldFail() {
        getMultiMapConfigReadOnly().addEntryListenerConfig(new EntryListenerConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingsEntryListenerConfigsOfReadOnlyMultiMapConfigShouldFail() {
        getMultiMapConfigReadOnly().setEntryListenerConfigs(Collections.singletonList(new EntryListenerConfig()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingBinaryOfReadOnlyMultiMapConfigShouldFail() {
        getMultiMapConfigReadOnly().setBinary(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingSyncBackupCountOfReadOnlyMultiMapConfigShouldFail() {
        getMultiMapConfigReadOnly().setSyncBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingBackupCountOfReadOnlyMultiMapConfigShouldFail() {
        getMultiMapConfigReadOnly().setBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingAsyncBackupCountOfReadOnlyMultiMapConfigShouldFail() {
        getMultiMapConfigReadOnly().setAsyncBackupCount(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingStatisticsEnabledOfReadOnlyMultiMapConfigShouldFail() {
        getMultiMapConfigReadOnly().setStatisticsEnabled(true);
    }
}
