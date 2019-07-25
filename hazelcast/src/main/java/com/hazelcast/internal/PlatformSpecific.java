/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal;

/**
 * Utility class to construct instances depending on the runtime platform.
 * Steps to introduce Java 6 / 8 alternative implementations:
 * <ol>
 *     <li>Define and use an interface in Hazelcast codebase</li>
 *     <li>Implement the interface for Java 6</li>
 *     <li>Implement the interface for Java 8+. Annotate this implementation with
 *     {@link RequiresJdk8}: animal-sniffer will not scan the annotated class or methods
 *     for Java 6 API compatibility, so use this annotation at the smallest possible scope
 *     </li>
 *     <li>Add a {@code createXYZ} public static method in {@code PlatformSpecific} utility
 *     class</li>
 *     <li>Use {@code PlatformSpecific.createXYZ} whenever you need a new instance</li>
 * </ol>
 *
 * The implementation of a constructor utility method would look something like this example:
 * <pre>
 * public static &lt;S&gt; MethodProbe createLongMethodProbe(Method method, Probe probe, int type) {
 *   if (JavaVersion.isAtLeast(JAVA_8)) {
 *     return new LongMethodProbeJdk8&lt;S&gt;(method, probe, type);
 *   } else {
 *     return new MethodProbe.LongMethodProbe&lt;S&gt;(method, probe, type);
 *   }
 * }
 * </pre>
 *
 * This should be tested in two separate tests, to ensure the proper instance is created
 * depending on Java runtime version, like this:
 * <pre>
 *     &#64;Test
 *     public void testJdk8Probe() {
 *       // ensure this test only runs on JDK 8+
 *       assumeJdk8OrNewer();
 *       MethodProbe jdk8Probe = PlatformSpecific.createLongMethodProbe(...);
 *       assertTrue(jdk8Probe instanceof LongMethodProbeJdk8.class);
 *     }
 * </pre>
 */
public final class PlatformSpecific {

    private PlatformSpecific() {

    }
}
