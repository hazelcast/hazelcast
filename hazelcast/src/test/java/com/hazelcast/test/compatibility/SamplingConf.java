/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.TestEnvironment;

import static com.hazelcast.test.HazelcastTestSupport.randomString;

public final class SamplingConf {

    public static final String FILE_NAME = TestEnvironment.getSerializedClassNamesPath() + randomString();
    public static final String SAMPLES_FILE_SUFFIX = ".samples";
    public static final String INDEX_FILE_SUFFIX = ".index";
    public static final String SCHEMA_FILE_SUFFIX = ".schema";

    private SamplingConf() {
    }
}
