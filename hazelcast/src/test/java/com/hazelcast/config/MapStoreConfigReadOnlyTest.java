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

import com.hazelcast.internal.config.MapStoreConfigReadOnly;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapStoreConfigReadOnlyTest {

    private MapStoreConfig getReadOnlyConfig() {
        return new MapStoreConfigReadOnly(new MapStoreConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setClassNameOfReadOnlyMapStoreConfigShouldFail() {
        getReadOnlyConfig().setClassName("myClassName");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setFactoryClassNameOfReadOnlyMapStoreConfigShouldFail() {
        getReadOnlyConfig().setFactoryClassName("myFactoryClassName");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setWriteDelaySecondsOfReadOnlyMapStoreConfigShouldFail() {
        getReadOnlyConfig().setWriteDelaySeconds(5);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setWriteBatchSizeOfReadOnlyMapStoreConfigShouldFail() {
        getReadOnlyConfig().setWriteBatchSize(3);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setEnabledOfReadOnlyMapStoreConfigShouldFail() {
        getReadOnlyConfig().setEnabled(false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setImplementationOfReadOnlyMapStoreConfigShouldFail() {
        getReadOnlyConfig().setImplementation(new Object());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setInitialLoadModeOfReadOnlyMapStoreConfigShouldFail() {
        getReadOnlyConfig().setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setFactoryImplementationOfReadOnlyMapStoreConfigShouldFail() {
        getReadOnlyConfig().setFactoryImplementation(new Object());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setPropertyOfReadOnlyMapStoreConfigShouldFail() {
        getReadOnlyConfig().setProperty("name", "value");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setPropertiesOfReadOnlyMapStoreConfigShouldFail() {
        getReadOnlyConfig().setProperties(new Properties());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setWriteCoalescingOfReadOnlyMapStoreConfigShouldFail() {
        getReadOnlyConfig().setWriteCoalescing(true);
    }
}
