/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cache.nearcache;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(SlowTest.class)
public class ClientCacheNearCachePreloaderSlowTest extends ClientCacheNearCachePreloaderTest {

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameter(value = 1)
    public boolean invalidationOnChange;

    @Parameterized.Parameter(value = 2)
    public boolean serializeKeys;

    @Parameterized.Parameters(name = "format:{0} invalidationOnChange:{1} serializeKeys:{2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, false, true},
                {InMemoryFormat.BINARY, false, false},
                {InMemoryFormat.BINARY, true, true},
                {InMemoryFormat.BINARY, true, false},

                {InMemoryFormat.OBJECT, false, true},
                {InMemoryFormat.OBJECT, false, false},
                {InMemoryFormat.OBJECT, true, true},
                {InMemoryFormat.OBJECT, true, false},
        });
    }

    @Before
    public void setUp() {
        nearCacheConfig = getNearCacheConfig(inMemoryFormat, serializeKeys, invalidationOnChange, KEY_COUNT,
                storeFile.getParent());
    }
}
