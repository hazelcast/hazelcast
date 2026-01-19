/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.archunit;

import com.tngtech.archunit.core.domain.JavaAccess;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;

import java.util.Set;
import java.util.function.Consumer;

public class NotCreateInstanceWithNetworkCondition extends ArchCondition<JavaClass> {

    /**
     * These classes create instances with real network.
     * TestHazelcastInstanceFactory is allowed - it creates instances with mock network according to setup.
     */
    private static final Set<String> HAZELCAST_FACTORY_CLASSES = Set.of(
            "com.hazelcast.core.Hazelcast",
            "com.hazelcast.instance.impl.HazelcastInstanceFactory",
            // TestAwareInstanceFactory sets cluster name to method name which is not truly unique
            // and its javadoc does not recommend using it with ParallelJVMTest
            "com.hazelcast.test.TestAwareInstanceFactory");

    private static final Set<String> HAZELCAST_FACTORY_METHODS = Set.of(
            "newHazelcastInstance",
            "getOrCreateHazelcastInstance");

    NotCreateInstanceWithNetworkCondition() {
        super("not create instance with network");
    }

    @Override
    public void check(JavaClass clazz, ConditionEvents events) {
        Consumer<JavaAccess> checkForbiddenReferences = mc -> {
            if (HAZELCAST_FACTORY_CLASSES.contains(mc.getTarget().getOwner().getName())
                    && HAZELCAST_FACTORY_METHODS.contains(mc.getTarget().getName())) {
                events.add(SimpleConditionEvent.violated(clazz,
                        String.format("Test class %s may create network-enabled instance in %s",
                                clazz.getName(), mc.getDescription())));
            }
        };
        // check also superclasses - they can have some initialization logic or helper methods
        clazz.getClassHierarchy().forEach(superclass -> {
            // find both normal invocation and references like Hazelcast::newHazelcastInstance
            superclass.getMethodCallsFromSelf().forEach(checkForbiddenReferences);
            superclass.getMethodReferencesFromSelf().forEach(checkForbiddenReferences);
        });
    }

    public static NotCreateInstanceWithNetworkCondition notCreateInstanceWithNetwork() {
        return new NotCreateInstanceWithNetworkCondition();
    }
}
