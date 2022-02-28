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

package com.hazelcast.internal.yaml;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests the error cases of the {@link YamlDomBuilder}
 *
 * The positive cases are tested in {@link YamlTest}
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class YamlDomBuilderNegativeTest {
    private static final Object NOT_A_MAP = new Object();

    @Test(expected = YamlException.class)
    public void testBuildFromNullDocumentThrows() {
        YamlDomBuilder.build(null);
    }

    @Test(expected = YamlException.class)
    public void testBuildFromNullDocumentAndRootNameThrows() {
        YamlDomBuilder.build(null, "root");
    }

    @Test(expected = YamlException.class)
    public void testBuildFromNotMapDocumentThrows() {
        YamlDomBuilder.build(NOT_A_MAP);
    }

    @Test(expected = YamlException.class)
    public void testBuildFromNotMapDocumentAndRootNameThrows() {
        YamlDomBuilder.build(NOT_A_MAP, "root");
    }

    @Test(expected = YamlException.class)
    public void testBuildFromMapDocumentRootNameNotFoundThrows() {
        Map<String, Object> documentMap = new HashMap<String, Object>();
        documentMap.put("not-root", new Object());
        YamlDomBuilder.build(documentMap, "root");
    }

    @Test(expected = YamlException.class)
    public void testBuildFromMapDocumentInvalidScalarThrows() {
        Map<String, Object> documentMap = new HashMap<String, Object>();
        documentMap.put("root", new Object());
        YamlDomBuilder.build(documentMap, "root");
    }
}
