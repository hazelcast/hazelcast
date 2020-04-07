/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.usercodedeployment.impl.filter;

import com.hazelcast.config.Config;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.FilteringClassLoader;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import usercodedeployment.ClassWithTwoInnerClasses;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UserCodeDeploymentBasicTest extends UserCodeDeploymentAbstractTest {

    @Parameterized.Parameter
    public volatile UserCodeDeploymentConfig.ClassCacheMode classCacheMode;

    @Parameterized.Parameters(name = "ClassCacheMode:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {UserCodeDeploymentConfig.ClassCacheMode.ETERNAL},
                {UserCodeDeploymentConfig.ClassCacheMode.OFF},
        });
    }

    @Override
    protected UserCodeDeploymentConfig.ClassCacheMode getClassCacheMode() {
        return classCacheMode;
    }

    @Test
    public void givenInnerClassOneIsCachedInServer1_whenInnerClassTwoIsRequested_thenServer1RespondsNull() {
        Config config = new Config();
        config.getUserCodeDeploymentConfig()
                .setEnabled(true);

        Config configWithoutEnclosingClass = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(asList("usercodedeployment"), null);
        configWithoutEnclosingClass.setClassLoader(filteringCL);
        configWithoutEnclosingClass.getUserCodeDeploymentConfig()
                .setEnabled(true);

        ClassWithTwoInnerClasses.StaticNestedIncrementingEntryProcessor<String> ep =
                new ClassWithTwoInnerClasses.StaticNestedIncrementingEntryProcessor<String>();

        factory = newFactory();
        HazelcastInstance instance1WithoutEp = factory.newHazelcastInstance(configWithoutEnclosingClass);

        HazelcastInstance instance2WithoutEp = factory.newHazelcastInstance(configWithoutEnclosingClass);

        // instance with ep
        factory.newHazelcastInstance(config);

        String mapName = randomName();
        IMap<String, Integer> map = instance1WithoutEp.getMap(mapName);
        String key = generateKeyOwnedBy(instance2WithoutEp);
        map.put(key, 0);
        map.executeOnEntries(ep);
        assertEquals(1, (int) map.get(key));

        ClassWithTwoInnerClasses.StaticNestedDecrementingEntryProcessor ep2 =
                new ClassWithTwoInnerClasses.StaticNestedDecrementingEntryProcessor();
        // executing ep on instance without that ep
        map.executeOnKey(key, ep2);
        assertEquals(0, (int) map.get(key));
    }

    protected TestHazelcastInstanceFactory newFactory() {
        return new TestHazelcastInstanceFactory();
    }
}
