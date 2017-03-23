/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IMap;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.FilteringClassLoader;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import usercodedeployment.IncrementingEntryProcessor;
import usercodedeployment.blacklisted.BlacklistedEP;
import usercodedeployment.whitelisted.WhitelistedEP;

import java.util.Collection;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class UserCodeDeploymentSmokeTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

    @Parameter
    public UserCodeDeploymentConfig.ClassCacheMode classCacheMode;

    @Parameters(name = "ClassCacheMode:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {UserCodeDeploymentConfig.ClassCacheMode.ETERNAL},
                {UserCodeDeploymentConfig.ClassCacheMode.OFF},
        });
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testUserCodeDeploymentIsDisabledByDefault() {
        // this test also validate the EP is filtered locally and has to be loaded from the other member
        Config i1Config = new Config();

        Config i2Config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
        i2Config.setClassLoader(filteringCL);

        IncrementingEntryProcessor incrementingEntryProcessor = new IncrementingEntryProcessor();
        executeSimpleTestScenario(i1Config, i2Config, incrementingEntryProcessor);
    }

    @Test
    public void givenSomeMemberCanAccessTheEP_whenTheEPIsFilteredLocally_thenItWillBeLoadedOverNetwork() {
        Config i1Config = new Config();
        i1Config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode);

        Config i2Config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
        i2Config.setClassLoader(filteringCL);
        i2Config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode);

        IncrementingEntryProcessor incrementingEntryProcessor = new IncrementingEntryProcessor();
        executeSimpleTestScenario(i1Config, i2Config, incrementingEntryProcessor);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void givenTheEPButItIsBlacklisted_whenTheEPIsFilteredLocally_thenItWillFailToLoadIt() {
        Config i1Config = new Config();
        i1Config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode);


        Config i2Config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
        i2Config.setClassLoader(filteringCL);
        i2Config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setBlacklistedPrefixes("usercodedeployment.blacklisted")
                .setClassCacheMode(classCacheMode);

        EntryProcessor<Integer, Integer> myEP = new BlacklistedEP();
        executeSimpleTestScenario(i1Config, i2Config, myEP);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void givenTheEPButItIsNotOnTheWhitelist_whenTheEPIsFilteredLocally_thenItWillFailToLoadIt() {
        Config i1Config = new Config();
        i1Config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode);

        Config i2Config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
        i2Config.setClassLoader(filteringCL);
        i2Config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setWhitelistedPrefixes("usercodedeployment.whitelisted")
                .setClassCacheMode(classCacheMode);

        EntryProcessor<Integer, Integer> myEP = new IncrementingEntryProcessor();
        executeSimpleTestScenario(i1Config, i2Config, myEP);
    }

    @Test
    public void givenTheEPIsOnTheWhitelist_whenTheEPIsFilteredLocally_thenItWillLoadIt() {
        Config i1Config = new Config();
        i1Config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode);

        Config i2Config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
        i2Config.setClassLoader(filteringCL);
        i2Config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setWhitelistedPrefixes("usercodedeployment.whitelisted, usercodedeployment")
                .setClassCacheMode(classCacheMode);

        EntryProcessor<Integer, Integer> myEP = new WhitelistedEP();
        executeSimpleTestScenario(i1Config, i2Config, myEP);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void givenProviderFilterUsesMemberAttribute_whenNoMemberHasMatchingAttribute_thenClassLoadingRequestFails() {
        Config i1Config = new Config();
        i1Config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode);

        Config i2Config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
        i2Config.setClassLoader(filteringCL);
        i2Config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setProviderFilter("HAS_ATTRIBUTE:foo")
                .setClassCacheMode(classCacheMode);

        EntryProcessor<Integer, Integer> myEP = new IncrementingEntryProcessor();
        executeSimpleTestScenario(i1Config, i2Config, myEP);
    }

    @Test
    public void givenProviderFilterUsesMemberAttribute_whenSomeMemberHasMatchingAttribute_thenClassLoadingRequestSucceed() {
        Config i1Config = new Config();
        i1Config.getMemberAttributeConfig().setStringAttribute("foo", "bar");
        i1Config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode);

        Config i2Config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
        i2Config.setClassLoader(filteringCL);
        i2Config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setProviderFilter("HAS_ATTRIBUTE:foo")
                .setClassCacheMode(classCacheMode);

        EntryProcessor<Integer, Integer> myEP = new IncrementingEntryProcessor();
        executeSimpleTestScenario(i1Config, i2Config, myEP);
    }

    private void executeSimpleTestScenario(Config i1Config, Config i2Config, EntryProcessor<Integer, Integer> ep) {
        int keyCount = 100;

        HazelcastInstance i1 = factory.newHazelcastInstance(i1Config);
        HazelcastInstance i2 = factory.newHazelcastInstance(i2Config);
        IMap<Integer, Integer> map = i1.getMap(randomName());

        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0);
        }
        map.executeOnEntries(ep);
        for (int i = 0; i < keyCount; i++) {
            assertEquals(1, (int) map.get(i));
        }
    }
}
