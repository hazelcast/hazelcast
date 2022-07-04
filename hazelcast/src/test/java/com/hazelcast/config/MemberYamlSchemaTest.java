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

package com.hazelcast.config;

import org.junit.runners.Parameterized;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class MemberYamlSchemaTest
        extends AbstractYamlSchemaTest {

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> buildTestcases() {
        return Stream.concat(
                buildTestcases("com/hazelcast/config/yaml/testcases/member/").stream(),
                buildTestcases("com/hazelcast/config/yaml/testcases/common/").stream()
        ).collect(toList());
    }

}
