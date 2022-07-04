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

package com.hazelcast.wan.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static java.util.Arrays.asList;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WanCustomPublisherMapTest extends AbstractWanCustomPublisherMapTest {

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameters(name = "inMemoryFormat:{0}")
    public static Collection<Object[]> data() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT}
        });
    }

    @Override
    protected Config getConfig() {
        WanReplicationConfig wanReplicationConfig = new WanReplicationConfig()
                .setName("dummyWan")
                .addCustomPublisherConfig(getPublisherConfig());

        WanReplicationRef wanRef = new WanReplicationRef()
                .setName("dummyWan")
                .setMergePolicyClassName(PassThroughMergePolicy.class.getName());

        MapConfig mapConfig = new MapConfig("default")
                .setInMemoryFormat(inMemoryFormat)
                .setWanReplicationRef(wanRef);

        return smallInstanceConfig()
                .addWanReplicationConfig(wanReplicationConfig)
                .addMapConfig(mapConfig);
    }
}
