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

package com.hazelcast.test.annotation;

import com.hazelcast.test.TestEnvironment;

/**
 * Mark a test as a compatibility test, suitable to be executed on a cluster with mixed Hazelcast versions.
 * To actually perform a compatibility test, you need to start test execution with system property
 * "hazelcast.test.compatibility=true".
 *
 * @see TestEnvironment#isRunningCompatibilityTest()
 */
public final class CompatibilityTest {
}
