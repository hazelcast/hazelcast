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

package com.hazelcast.internal.usercodedeployment.impl.filter;

import com.hazelcast.config.Config;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import usercodedeployment.ClassWithTwoInnerClasses;
import usercodedeployment.DomainClassWithInnerClass;
import usercodedeployment.EntryProcessorWithAnonymousAndInner;
import usercodedeployment.IncrementingEntryProcessor;
import usercodedeployment.blacklisted.BlacklistedEP;
import usercodedeployment.whitelisted.WhitelistedEP;

import java.util.Collection;

import static com.hazelcast.test.starter.HazelcastStarterUtils.assertInstanceOfByClassName;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UserCodeDeploymentBasicTest extends HazelcastTestSupport {

    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-no-stacktrace.xml");

    private TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();
    @Parameterized.Parameter
    public volatile UserCodeDeploymentConfig.ClassCacheMode classCacheMode;

    @Parameterized.Parameters(name = "ClassCacheMode:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {UserCodeDeploymentConfig.ClassCacheMode.ETERNAL},
                {UserCodeDeploymentConfig.ClassCacheMode.OFF},
        });
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testUserCodeDeploymentIsDisabledByDefault() {
        // this test also validate the EP is filtered locally and has to be loaded from the other member
        Config i1Config = new Config();

        Config i2Config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
        i2Config.setClassLoader(filteringCL);

        IncrementingEntryProcessor incrementingEntryProcessor = new IncrementingEntryProcessor();
        try {
            executeSimpleTestScenario(i1Config, i2Config, incrementingEntryProcessor);
            fail();
        } catch (Exception e) {
            assertInstanceOfByClassName(HazelcastSerializationException.class.getName(), e);
        }
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

    @Test
    public void givenSomeMemberCanAccessTheEP_whenTheEPIsFilteredLocally_thenItWillBeLoadedOverNetwork_anonymousInnerClasses() {
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

        EntryProcessorWithAnonymousAndInner incrementingEntryProcessor = new EntryProcessorWithAnonymousAndInner();
        executeSimpleTestScenario(i1Config, i2Config, incrementingEntryProcessor);
    }

    @Test
    public void testInnerClassFetchedFirst_thenMainClassFetchedFromRemote() {
        Config i1Config = new Config();
        i1Config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode);

        Config filteredConfig = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
        filteredConfig.setClassLoader(filteringCL);
        filteredConfig.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode);

        HazelcastInstance instanceWithClasses = factory.newHazelcastInstance(i1Config);
        HazelcastInstance instanceWithoutTheClasses = factory.newHazelcastInstance(filteredConfig);

        IMap<Object, Object> map = instanceWithClasses.getMap("test");
        DomainClassWithInnerClass mainDomainObject = new DomainClassWithInnerClass(new DomainClassWithInnerClass.InnerClass(2));
        map.put("main", mainDomainObject);
        DomainClassWithInnerClass.InnerClass innerObject = new DomainClassWithInnerClass.InnerClass(1);
        map.put("inner", innerObject);

        IMap<Object, Object> map2 = instanceWithoutTheClasses.getMap("test");
        map2.get("inner");
        map2.get("main");
    }

    @Test
    public void testMainClassFetchedFirst_thenInnerlassFetchedFromRemote() {
        Config i1Config = new Config();
        i1Config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode);

        Config filteredConfig = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
        filteredConfig.setClassLoader(filteringCL);
        filteredConfig.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode);

        HazelcastInstance instanceWithClasses = factory.newHazelcastInstance(i1Config);
        HazelcastInstance instanceWithoutTheClasses = factory.newHazelcastInstance(filteredConfig);

        IMap<Object, Object> map = instanceWithClasses.getMap("test");
        DomainClassWithInnerClass mainDomainObject = new DomainClassWithInnerClass(new DomainClassWithInnerClass.InnerClass(2));
        map.put("main", mainDomainObject);
        DomainClassWithInnerClass.InnerClass innerObject = new DomainClassWithInnerClass.InnerClass(1);
        map.put("inner", innerObject);

        IMap<Object, Object> map2 = instanceWithoutTheClasses.getMap("test");
        map2.get("main");
        map2.get("inner");
    }

    @Test
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

        EntryProcessor<Integer, Integer, Integer> myEP = new BlacklistedEP();
        try {
            executeSimpleTestScenario(i1Config, i2Config, myEP);
            fail();
        } catch (Exception e) {
            assertInstanceOfByClassName(HazelcastSerializationException.class.getName(), e);
        }
    }

    @Test
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

        EntryProcessor<Integer, Integer, Integer> myEP = new IncrementingEntryProcessor();
        try {
            executeSimpleTestScenario(i1Config, i2Config, myEP);
            fail();
        } catch (Exception e) {
            assertInstanceOfByClassName(HazelcastSerializationException.class.getName(), e);
        }
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

        EntryProcessor<Integer, Integer, Integer> myEP = new WhitelistedEP();
        executeSimpleTestScenario(i1Config, i2Config, myEP);
    }

    @Test
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

        EntryProcessor<Integer, Integer, Integer> myEP = new IncrementingEntryProcessor();
        try {
            executeSimpleTestScenario(i1Config, i2Config, myEP);
            fail();
        } catch (Exception e) {
            assertInstanceOfByClassName(HazelcastSerializationException.class.getName(), e);
        }
    }

    @Test
    public void givenProviderFilterUsesMemberAttribute_whenSomeMemberHasMatchingAttribute_thenClassLoadingRequestSucceed() {
        Config i1Config = new Config();
        i1Config.getMemberAttributeConfig().setAttribute("foo", "bar");
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

        EntryProcessor<Integer, Integer, Integer> myEP = new IncrementingEntryProcessor();
        executeSimpleTestScenario(i1Config, i2Config, myEP);
    }

    protected void executeSimpleTestScenario(Config config,
                                             Config epFilteredConfig,
                                             EntryProcessor<Integer, Integer, Integer> ep) {
        int keyCount = 100;

        HazelcastInstance instanceWithNewEp = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(epFilteredConfig);

        IMap<Integer, Integer> map = instanceWithNewEp.getMap(randomName());

        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0);
        }
        map.executeOnEntries(ep);
        for (int i = 0; i < keyCount; i++) {
            assertEquals(1, (int) map.get(i));
        }
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
}
