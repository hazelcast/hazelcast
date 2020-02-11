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
import net.bytebuddy.jar.asm.Opcodes;

import java.lang.instrument.Instrumentation;

public final class CompatibilityTestUtils {

    // When running a compatibility test, all com.hazelcast.* classes are transformed so that none are
    // loaded with final modifier to allow subclass proxying.
    public static void attachFinalRemovalAgent() {
        Instrumentation instrumentation = ByteBuddyAgent.install();
        new AgentBuilder.Default().with(AgentBuilder.TypeStrategy.Default.REDEFINE)
                                  .with(AgentBuilder.InitializationStrategy.NoOp.INSTANCE)
                                  .type(target -> target.getName().startsWith("com.hazelcast"))
                                  .transform((builder, typeDescription, classLoader, module) -> {
                                      int actualModifiers = typeDescription.getActualModifiers(false);
                                      // unset final modifier
                                      int nonFinalModifiers = actualModifiers & ~Opcodes.ACC_FINAL;
                                      if (actualModifiers != nonFinalModifiers) {
                                          return builder.modifiers(nonFinalModifiers);
                                      } else {
                                          return builder;
                                      }
                                  }).installOn(instrumentation);
    }
}
