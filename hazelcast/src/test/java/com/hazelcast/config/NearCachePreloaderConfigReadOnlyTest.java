/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.config.NearCachePreloaderConfigReadOnly;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({ParallelJVMTest.class})
public class NearCachePreloaderConfigReadOnlyTest {

    private NearCachePreloaderConfig getReadOnlyConfig() {
        return new NearCachePreloaderConfigReadOnly(new NearCachePreloaderConfig());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setEnabledOnReadOnlyNearCachePreloaderConfigShouldFail() {
        getReadOnlyConfig().setEnabled(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setDirectoryOnReadOnlyNearCachePreloaderConfigShouldFail() {
        getReadOnlyConfig().setDirectory("myDirectory");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setStoreInitialDelaySecondsOnReadOnlyNearCachePreloaderConfigShouldFail() {
        getReadOnlyConfig().setStoreInitialDelaySeconds(5);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setStoreIntervalSecondsOnReadOnlyNearCachePreloaderConfigShouldFail() {
        getReadOnlyConfig().setStoreIntervalSeconds(5);
    }
}
