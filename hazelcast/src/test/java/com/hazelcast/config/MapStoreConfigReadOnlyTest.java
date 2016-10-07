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

import java.util.Properties;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MapStoreConfigReadOnlyTest {

    private MapStoreConfigReadOnly getMapStoreConfigReadOnly() {
        return new MapStoreConfig().getAsReadOnly();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingClassNameOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setClassName("myClassName");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingFactoryClassNameOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setFactoryClassName("myFactoryClassName");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingWriteDelaySecondsOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setWriteDelaySeconds(5);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingWriteBatchSizeOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setWriteBatchSize(3);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingsEnabledOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setEnabled(false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingImplementationOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setImplementation(new Object());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingInitialLoadModeOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingFactoryImplementationOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setFactoryImplementation(new Object());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingPropertyOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setProperty("name", "value");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingPropertiesOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setProperties(new Properties());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void settingWriteCoalescingOfReadOnlyMapStoreConfigShouldFail() {
        getMapStoreConfigReadOnly().setWriteCoalescing(true);
    }
}
