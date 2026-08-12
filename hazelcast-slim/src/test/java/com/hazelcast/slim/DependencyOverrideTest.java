/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.slim;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Verifies the defining property of hazelcast-slim: the third-party libraries that the shaded
 * {@code com.hazelcast:hazelcast} jar relocates under {@code com.hazelcast.shaded.*} are present here
 * under their original package names. That is what lets a consumer override them (e.g. to pull a
 * patched version for a CVE) via standard Maven dependency management — see
 * <a href="https://github.com/hazelcast/hazelcast/issues/26601">#26601</a>.
 *
 * <p>The assertions are intentionally version-agnostic: they check the package a class resolves to,
 * not which version is pinned, so the test does not need updating whenever a dependency is bumped.
 * Against the shaded jar the same classes resolve under {@code com.hazelcast.shaded.*}, so these
 * assertions would fail there.
 */
class DependencyOverrideTest {

    @Test
    void jacksonIsPresentAtItsOriginalPackageNotRelocated() {
        // In the shaded jar this class is com.hazelcast.shaded.com.fasterxml.jackson.databind.ObjectMapper.
        assertEquals("com.fasterxml.jackson.databind", ObjectMapper.class.getPackageName());
        assertFalse(ObjectMapper.class.getName().contains("shaded"),
                "Jackson appears to be relocated — this is hazelcast-slim, it must not be shaded");
    }

    @Test
    void snakeyamlIsPresentAtItsOriginalPackageNotRelocated() {
        // In the shaded jar this class is com.hazelcast.shaded.org.snakeyaml.engine.v2.api.Load.
        assertEquals("org.snakeyaml.engine.v2.api", Load.class.getPackageName());
        assertFalse(Load.class.getName().contains("shaded"),
                "SnakeYAML appears to be relocated — this is hazelcast-slim, it must not be shaded");
    }

    @Test
    void jacksonStillFunctionsRegardlessOfResolvedVersion() throws Exception {
        var mapper = new ObjectMapper();
        var json = mapper.writeValueAsString(Map.of("cluster-name", "slim-test", "port", 5701));

        @SuppressWarnings("unchecked")
        var parsed = (Map<String, Object>) mapper.readValue(json, Map.class);

        assertEquals("slim-test", parsed.get("cluster-name"));
        assertEquals(5701, parsed.get("port"));
    }

    @Test
    void snakeyamlStillFunctionsRegardlessOfResolvedVersion() {
        var load = new Load(LoadSettings.builder().build());

        @SuppressWarnings("unchecked")
        var result = (Map<String, Object>) load.loadFromString("hazelcast:\n  cluster-name: slim-test\n");

        assertEquals("slim-test", ((Map<?, ?>) result.get("hazelcast")).get("cluster-name"));
    }
}
