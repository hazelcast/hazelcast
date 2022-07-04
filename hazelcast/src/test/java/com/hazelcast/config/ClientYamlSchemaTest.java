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

import org.json.JSONObject;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class ClientYamlSchemaTest
        extends AbstractYamlSchemaTest {

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> buildTestcases() {
        return Stream.concat(
                buildTestcases("com/hazelcast/config/yaml/testcases/client/").stream(),
                buildTestcases("com/hazelcast/config/yaml/testcases/common/").stream()
                        .map(ClientYamlSchemaTest::changeInputRootElem)
        ).collect(toList());
    }

    private static Object[] changeInputRootElem(Object[] args) {
        JSONObject input = (JSONObject) args[1];
        Object hazelcastValue = input.getJSONObject("hazelcast");
        input.remove("hazelcast");
        input.put("hazelcast-client", hazelcastValue);
        amendInstancePointersForClient((JSONObject) args[2]);
        return new Object[]{args[0], input, args[2]};
    }

    private static void amendInstancePointersForClient(JSONObject error) {
        if (error == null) {
            return;
        }
        String pointerToViolation = error.optString("pointerToViolation");
        if (pointerToViolation != null) {
            error.put("pointerToViolation", pointerToViolation.replace("#/hazelcast", "#/hazelcast-client"));
            error.optJSONArray("causingExceptions")
                    .forEach(causingExcJson -> amendInstancePointersForClient((JSONObject) causingExcJson));
        }
    }
}
