package com.hazelcast.internal.distributedclassloading.impl.filter;

import com.hazelcast.config.Config;
import com.hazelcast.config.DistributedClassloadingConfig;
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
import distributedclassloading.IncrementingEntryProcessor;
import distributedclassloading.blacklisted.BlacklistedEP;
import distributedclassloading.whitelisted.WhitelistedEP;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class DistributedClassloadingSmokeTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @Parameterized.Parameter
    public DistributedClassloadingConfig.ClassCacheMode classCacheMode;

    @Parameterized.Parameters(name = "ClassCacheMode:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {DistributedClassloadingConfig.ClassCacheMode.ETERNAL},
                {DistributedClassloadingConfig.ClassCacheMode.OFF},
        });
    }

    @Before
    public void setUp() {
        factory = createHazelcastInstanceFactory(2);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void testDistributedClassloadingIsDisabledByDefault() {
        //this test also validate the EP is filtered locally and has to be loaded from the other member
        Config i1Config = new Config();

        Config i2Config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(asList("distributedclassloading"), null);
        i2Config.setClassLoader(filteringCL);

        IncrementingEntryProcessor incrementingEntryProcessor = new IncrementingEntryProcessor();
        executeSimpleTestScenario(i1Config, i2Config, incrementingEntryProcessor);
    }

    @Test
    public void givenSomeMemberCanAccessTheEP_whenTheEPIsFilteredLocally_thenItWillBeLoadedOverNetwork() {
        Config i1Config = new Config();
        i1Config.getDistributedClassloadingConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode);

        Config i2Config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(asList("distributedclassloading"), null);
        i2Config.setClassLoader(filteringCL);
        i2Config.getDistributedClassloadingConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode);

        IncrementingEntryProcessor incrementingEntryProcessor = new IncrementingEntryProcessor();
        executeSimpleTestScenario(i1Config, i2Config, incrementingEntryProcessor);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void givenTheEPButItIsBlacklisted_whenTheEPIsFilteredLocally_thenItWillFailToLoadIt() {
        Config i1Config = new Config();
        i1Config.getDistributedClassloadingConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode);


        Config i2Config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(asList("distributedclassloading"), null);
        i2Config.setClassLoader(filteringCL);
        i2Config.getDistributedClassloadingConfig()
                .setEnabled(true)
                .setBlacklistedPrefixes("distributedclassloading.blacklisted")
                .setClassCacheMode(classCacheMode);

        EntryProcessor<Integer, Integer> myEP = new BlacklistedEP();
        executeSimpleTestScenario(i1Config, i2Config, myEP);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void givenTheEPButItIsNotOnTheWhitelist_whenTheEPIsFilteredLocally_thenItWillFailToLoadIt() {
        Config i1Config = new Config();
        i1Config.getDistributedClassloadingConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode);


        Config i2Config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(asList("distributedclassloading"), null);
        i2Config.setClassLoader(filteringCL);
        i2Config.getDistributedClassloadingConfig()
                .setEnabled(true)
                .setWhitelistedPrefixes("Distributedclassloading.whitelisted")
                .setClassCacheMode(classCacheMode);

        EntryProcessor<Integer, Integer> myEP = new IncrementingEntryProcessor();
        executeSimpleTestScenario(i1Config, i2Config, myEP);
    }

    @Test
    public void givenTheEPIsOnTheWhitelist_whenTheEPIsFilteredLocally_thenItWillLoadIt() {
        Config i1Config = new Config();
        i1Config.getDistributedClassloadingConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode);

        Config i2Config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(asList("distributedclassloading"), null);
        i2Config.setClassLoader(filteringCL);
        i2Config.getDistributedClassloadingConfig()
                .setEnabled(true)
                .setWhitelistedPrefixes("distributedclassloading.whitelisted, distributedclassloading")
                .setClassCacheMode(classCacheMode);

        EntryProcessor<Integer, Integer> myEP = new WhitelistedEP();
        executeSimpleTestScenario(i1Config, i2Config, myEP);
    }

    @Test(expected = HazelcastSerializationException.class)
    public void givenProviderFilterUsesMemberAttribute_whenNoMemberHasMatchingAttribute_thenClassLoadingRequestFails() {
        Config i1Config = new Config();
        i1Config.getDistributedClassloadingConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode);

        Config i2Config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(asList("distributedclassloading"), null);
        i2Config.setClassLoader(filteringCL);
        i2Config.getDistributedClassloadingConfig()
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
        i1Config.getDistributedClassloadingConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode);

        Config i2Config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(asList("distributedclassloading"), null);
        i2Config.setClassLoader(filteringCL);
        i2Config.getDistributedClassloadingConfig()
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
