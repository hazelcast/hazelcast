//package com.hazelcast.jet.impl.deployment;
//
//import com.hazelcast.client.test.TestHazelcastFactory;
//import com.hazelcast.core.HazelcastInstance;
//import com.hazelcast.test.HazelcastParallelClassRunner;
//import com.hazelcast.test.TestHazelcastInstanceFactory;
//import com.hazelcast.test.annotation.QuickTest;
//import org.junit.After;
//import org.junit.Ignore;
//import org.junit.Rule;
//import org.junit.experimental.categories.Category;
//import org.junit.rules.Timeout;
//import org.junit.runner.RunWith;
//
//@Category(QuickTest.class)
//@RunWith(HazelcastParallelClassRunner.class)
//@Ignore
//public class ClientDeploymentTest extends AbstractDeploymentTest {
//
//    @Rule
//    public final Timeout timeoutRule = Timeout.seconds(360);
//
//    private TestHazelcastFactory factory = new TestHazelcastFactory();
//
//    @After
//    public void tearDown() {
//        factory.terminateAll();
//    }
//
//
//    @Override
//    TestHazelcastInstanceFactory getFactory() {
//        return factory;
//    }
//
//    @Override
//    HazelcastInstance getHazelcastInstance() {
//        return factory.newHazelcastClient();
//    }
//}
