package com.hazelcast.jca;


import com.hazelcast.core.HazelcastInstance;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * This test class is to ensure a contract of {@link com.hazelcast.jca.HazelcastConnection} is aligned with
 * {@link com.hazelcast.core.HazelcastInstance}
 *
 */
public class HazecastConnectionContractTest {

    /**
     * A list of method from {@link com.hazelcast.core.HazelcastInstance} we don't want in {@link com.hazelcast.jca.HazelcastConnection}
     * If a method from this list is missing in {@link com.hazelcast.jca.HazelcastConnection} tests should still pass
     *
     */
    private Set<String> hazelcastInstanceOnlyMethods = new HashSet<String>(Arrays.asList(
            "getLifecycleService",              //lifecycle is controlled by JCA container
            "shutdown",                         //we don't want to shutdown instance from client code
            "newTransactionContext",            //transactions are controlled by a container
            "getLock",                          //there is a deprecated method getLock(String) in HazelcastInstance

            "getExecutorService",
            "getAtomicReference",
            "getLocalEndpoint",
            "getIdGenerator",
            "getReplicatedMap",
            "getCluster",
            "getConfig",
            "getName",
            "getUserContext",
            "getDistributedObject",
            "getDistributedObjects",
            "executeTransaction",
            "getJobTracker",
            "getPartitionService",
            "getLoggingService",
            "getClientService",
            "removeDistributedObjectListener",
            "addDistributedObjectListener"
    ));

    @Test
    public void verifyContractsOfMethods() {
        for (Method method : getPublicMethods(HazelcastInstance.class)) {
            boolean isIgnored = isMethodIgnored(method);
            boolean hasSameContract = hasMethodWithSameContract(HazelcastConnection.class, method);

            assertTrue(HazelcastConnection.class.getName()+" doesn't have method "+method+". Is it a new method?",
                    isIgnored || hasSameContract);
        }
    }

    private boolean isMethodIgnored(Method method) {
        return hazelcastInstanceOnlyMethods.contains(method.getName());
    }

    private boolean hasMethodWithSameContract(Class<?> clazz, Method method) {
        try {
            Method found = clazz.getMethod(method.getName(), method.getParameterTypes());
            boolean sameReturnType = method.getReturnType().equals(found.getReturnType());
            boolean sameModifiers = method.getModifiers() == found.getModifiers();

            return sameReturnType && sameModifiers;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private Method[] getPublicMethods(Class<?> clazz) {
        return clazz.getMethods();
    }
}