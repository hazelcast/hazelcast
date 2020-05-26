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

package com.hazelcast.test.compatibility;

import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.utility.JavaModule;

import java.lang.instrument.Instrumentation;

public final class CompatibilityTestUtils {

    /**
     * System property to override the other hazelcast version to be used by
     * compatibility tests running with other releases.
     * <p>
     * Set this system property to a single version,
     * e.g. {@code -Dhazelcast.test.compatibility.otherVersion=4.0}.
     */
    public static final String COMPATIBILITY_TEST_OTHER_VERSION = "hazelcast.test.compatibility.otherVersion";

    private static final String DEFAULT_OTHER_VERSION = "3.12.8-migration";

    // When running a compatibility test, all com.hazelcast.* classes are transformed so that none are
    // loaded with final modifier to allow subclass proxying.
    public static void attachFinalRemovalAgent() {
        Instrumentation instrumentation = ByteBuddyAgent.install();
        new AgentBuilder.Default()
                .disableClassFormatChanges()
                .type(new ElementMatcher<TypeDescription>() {
                    @Override
                    public boolean matches(TypeDescription target) {
                        return target.getName().startsWith("com.hazelcast");
                    }
                })
                .transform(new AgentBuilder.Transformer() {
                    @Override
                    public DynamicType.Builder<?> transform(DynamicType.Builder<?> builder,
                                                            TypeDescription typeDescription,
                                                            ClassLoader classLoader, JavaModule module) {
                        int actualModifiers = typeDescription.getActualModifiers(false);
                        // unset final modifier
                        int nonFinalModifiers = actualModifiers & ~Opcodes.ACC_FINAL;
                        return builder.modifiers(nonFinalModifiers);
                    }
                })
                .installOn(instrumentation);
    }

    /**
     * Resolves which version will be used for compatibility tests using the next
     * Hazelcast release.
     * <ol>
     * <li>look for system property override</li>
     * <li>fallback to 4.0</li>
     * </ol>
     */
    public static String resolveOtherVersion() {
        String systemPropertyOverride = System.getProperty(COMPATIBILITY_TEST_OTHER_VERSION);
        return systemPropertyOverride != null ? systemPropertyOverride : DEFAULT_OTHER_VERSION;
    }
}
