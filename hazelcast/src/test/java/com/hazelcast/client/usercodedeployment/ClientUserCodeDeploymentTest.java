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

package com.hazelcast.client.usercodedeployment;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import usercodedeployment.CapitalizingFirstNameExtractor;
import usercodedeployment.DomainClassWithInnerClass;
import usercodedeployment.EntryProcessorWithAnonymousAndInner;
import usercodedeployment.IncrementingEntryProcessor;
import usercodedeployment.Person;
import usercodedeployment.SampleBaseClass;
import usercodedeployment.SampleSubClass;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.query.Predicates.equal;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientUserCodeDeploymentTest extends ClientTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() throws Exception {
        factory.terminateAll();
    }

    @Parameterized.Parameter(0)
    public UserCodeDeploymentConfig.ClassCacheMode classCacheMode;
    @Parameterized.Parameter(1)
    public UserCodeDeploymentConfig.ProviderMode providerMode;


    @Parameters(name = "ClassCacheMode:{0}, ProviderMode:{1}")
    public static Collection<Object[]> parameters() {
        LinkedList<Object[]> parameters = new LinkedList<Object[]>();
        for (UserCodeDeploymentConfig.ClassCacheMode classCacheMode : UserCodeDeploymentConfig.ClassCacheMode.values()) {
            for (UserCodeDeploymentConfig.ProviderMode providerMode : UserCodeDeploymentConfig.ProviderMode.values()) {
                parameters.add(new Object[]{classCacheMode, providerMode});
            }
        }
        return parameters;
    }

    private Config createNodeConfig() {
        Config config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
        config.setClassLoader(filteringCL);
        config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode)
                .setProviderMode(providerMode);
        return config;
    }

    private ClientConfig createClientConfig() {
        ClientConfig config = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass("usercodedeployment.IncrementingEntryProcessor");
        config.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        return config;
    }

    @Test
    public void testSingleMember() {
        ClientConfig clientConfig = createClientConfig();
        Config config = createNodeConfig();

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        assertCodeDeploymentWorking(client, new IncrementingEntryProcessor());
    }

    @Test
    public void testWithMultipleMembers() {
        ClientConfig clientConfig = createClientConfig();
        Config config = createNodeConfig();

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        factory.newHazelcastInstance(config);

        assertCodeDeploymentWorking(client, new IncrementingEntryProcessor());
    }

    @Test
    public void testWithMultipleMembersAtStart() {
        ClientConfig clientConfig = createClientConfig();
        Config config = createNodeConfig();

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        assertCodeDeploymentWorking(client, new IncrementingEntryProcessor());
    }

    @Test
    public void testWithMultipleNodes_clientReconnectsToNewNode() throws InterruptedException {
        ClientConfig clientConfig = createClientConfig();
        Config config = createNodeConfig();

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        factory.newHazelcastInstance(config);

        ReconnectListener reconnectListener = new ReconnectListener();
        client.getLifecycleService().addLifecycleListener(reconnectListener);

        factory.shutdownAllMembers();
        factory.newHazelcastInstance(config);

        assertOpenEventually(reconnectListener.reconnectedLatch);
        assertCodeDeploymentWorking(client, new IncrementingEntryProcessor());
    }

    private void assertCodeDeploymentWorking(HazelcastInstance client, EntryProcessor entryProcessor) {
        int keyCount = 100;
        IMap<Integer, Integer> map = client.getMap(randomName());

        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0);
        }

        int incrementCount = 5;
        //doing the call a few times so that the invocation can be done on different members
        for (int i = 0; i < incrementCount; i++) {
            map.executeOnEntries(entryProcessor);
        }

        for (int i = 0; i < keyCount; i++) {
            assertEquals(incrementCount, (int) map.get(i));
        }
    }

    @Test
    public void testWithMultipleMembers_anonymousAndInnerClasses() {
        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addJar("EntryProcessorWithAnonymousAndInner.jar");
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));

        Config config = createNodeConfig();

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        factory.newHazelcastInstance(config);

        assertCodeDeploymentWorking(client, new EntryProcessorWithAnonymousAndInner());
    }

    @Test
    public void testCustomAttributeExtractor() {
        String mapName = randomMapName();
        String attributeName = "syntheticAttribute"; //this attribute does not exist in the domain class

        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass(CapitalizingFirstNameExtractor.class);
        clientUserCodeDeploymentConfig.addClass(Person.class);
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));

        Config config = createNodeConfig();
        config.getMapConfig(mapName).addAttributeConfig(new AttributeConfig(attributeName, "usercodedeployment.CapitalizingFirstNameExtractor"));

        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        IMap<Integer, Person> map = client.getMap(mapName);
        map.put(0, new Person("ada"));
        map.put(1, new Person("non-ada"));

        Set<Map.Entry<Integer, Person>> results = map.entrySet(equal(attributeName, "ADA"));
        assertEquals(1, results.size());
        assertEquals("ada", results.iterator().next().getValue().getName());
    }

    @Test
    public void testWithParentAndChildClassesWorksIndependentOfOrder_childFirst() {
        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass(SampleSubClass.class);
        clientUserCodeDeploymentConfig.addClass(SampleBaseClass.class);
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));

        factory.newHazelcastInstance(createNodeConfig());
        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testWithParentAndChildClassesWorksIndependentOfOrder_parentFirst() {
        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass(SampleBaseClass.class);
        clientUserCodeDeploymentConfig.addClass(SampleSubClass.class);
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));

        factory.newHazelcastInstance(createNodeConfig());
        factory.newHazelcastClient(clientConfig);
    }


    @Test
    public void testWithParentAndChildClassesWorksIndependentOfOrder_withChildParentJar() {
        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        /*child parent jar contains two classes as follows. This classes are not put into code base on purpose,
        in order not to effect the test. Child class is loaded first when reading via JarInputStream.getNextJarEntry, which
        is the case we wanted to test.

        package usercodedeployment;
        import java.io.Serializable;
        public class ParentClass implements Serializable, Runnable {
            @Override
            public void run() {

            }
        }

        package usercodedeployment;
        public class AChildClass extends AParentClass {
        }

         */
        clientUserCodeDeploymentConfig.addJar("ChildParent.jar");
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));

        factory.newHazelcastInstance(createNodeConfig());
        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testWithMainAndInnerClassesWorksIndependentOfOrder_withInnerFirst() {
        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass(DomainClassWithInnerClass.InnerClass.class);
        clientUserCodeDeploymentConfig.addClass(DomainClassWithInnerClass.class);
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));

        factory.newHazelcastInstance(createNodeConfig());
        factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testWithMainAndInnerClassesWorksIndependentOfOrder_withMainFirst() {
        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass(DomainClassWithInnerClass.class);
        clientUserCodeDeploymentConfig.addClass(DomainClassWithInnerClass.InnerClass.class);
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));

        factory.newHazelcastInstance(createNodeConfig());
        factory.newHazelcastClient(clientConfig);
    }
}
