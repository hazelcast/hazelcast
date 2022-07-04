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

package com.hazelcast.gcp;

import org.junit.Test;

import static com.hazelcast.gcp.Utils.lastPartOf;
import static com.hazelcast.gcp.Utils.splitByComma;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;

public class UtilsTest {
    @Test
    public void splitByCommaTest() {
        assertEquals(asList("project1", "project2"), splitByComma("project1,project2"));
        assertEquals(asList("project1", "project2"), splitByComma("    project1 ,  project2 "));
        assertEquals(asList("project1"), splitByComma("project1"));
        assertEquals(emptyList(), splitByComma(null));
    }

    @Test
    public void lastPartOfTest() {
        assertEquals("us-east1-a", lastPartOf("https://www.googleapis.com/compute/v1/projects/projectId/zones/us-east1-a", "/"));
        assertEquals("", lastPartOf("", ""));
        assertEquals("", lastPartOf("", "/"));
    }
}
