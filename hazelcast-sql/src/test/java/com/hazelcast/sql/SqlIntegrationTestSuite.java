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

package com.hazelcast.sql;

import com.googlecode.junittoolbox.WildcardPatternSuite;
import org.junit.runner.RunWith;

/**
 * A test suite of all SQL integration tests. These tests are run from Jet
 * where the HazelcastInstance is one from a Jet cluster.
 */
@RunWith(WildcardPatternSuite.class)
@com.googlecode.junittoolbox.SuiteClasses("**/*Test.class")
public class SqlIntegrationTestSuite {
}
