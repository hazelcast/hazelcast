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

package com.hazelcast.buildutils;

import com.hazelcast.buildutils.HazelcastManifestTransformer.PackageDefinition;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static java.lang.String.format;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PackageDefinitionTest {

    private Set<String> uses = emptySet();

    private PackageDefinition packageDefinition;
    private PackageDefinition packageDefinitionSameAttributes;
    private PackageDefinition packageDefinitionOtherResolutionOptional;
    private PackageDefinition packageDefinitionOtherVersion;

    private PackageDefinition packageDefinitionOtherPackageName;

    @Before
    public void setUp() {
        packageDefinition = new PackageDefinition("packageName", true, "3.8", uses);
        packageDefinitionSameAttributes = new PackageDefinition("packageName", true, "3.8", uses);
        packageDefinitionOtherResolutionOptional = new PackageDefinition("packageName", false, "3.8", uses);
        packageDefinitionOtherVersion = new PackageDefinition("packageName", true, "3.7", uses);

        packageDefinitionOtherPackageName = new PackageDefinition("otherPackageName", true, "3.8", uses);
    }

    @Test
    public void testBuildDefinition_withResolutionOptional() {
        String definition = new PackageDefinition("packageName", true, null, uses).buildDefinition(true);
        assertContains(definition, "packageName");
        assertContains(definition, "resolution:=optional");

        definition = new PackageDefinition("packageName", false, null, uses).buildDefinition(true);
        assertContains(definition, "packageName");
        assertNotContains(definition, "resolution:=");

        definition = new PackageDefinition("packageName", true, null, uses).buildDefinition(false);
        assertContains(definition, "packageName");
        assertNotContains(definition, "resolution:=");
    }

    @Test
    public void testBuildDefinition_withVersion() {
        String definition = new PackageDefinition("packageName", false, "3.8", uses).buildDefinition(false);
        assertContains(definition, "packageName");
        assertContains(definition, "version=3.8");

        definition = new PackageDefinition("packageName", false, null, uses).buildDefinition(false);
        assertContains(definition, "packageName");
        assertNotContains(definition, "version=");
    }

    @Test
    public void testBuildDefinition_withUses() {
        String definition = new PackageDefinition("packageName", false, null, singleton("myUsage")).buildDefinition(false);
        assertContains(definition, "packageName");
        assertContains(definition, "uses:=\"myUsage\"");

        definition = new PackageDefinition("packageName", false, null, uses).buildDefinition(false);
        assertContains(definition, "packageName");
        assertNotContains(definition, "uses:=");
    }

    @Test
    public void testEquals() {
        assertEquals(packageDefinition, packageDefinition);
        assertEquals(packageDefinition, packageDefinitionSameAttributes);
        assertEquals(packageDefinition, packageDefinitionOtherResolutionOptional);
        assertEquals(packageDefinition, packageDefinitionOtherVersion);

        assertNotEquals(packageDefinition, null);
        assertNotEquals(packageDefinition, new Object());

        assertNotEquals(packageDefinition, packageDefinitionOtherPackageName);
    }

    @Test
    public void testHashCode() {
        assertEquals(packageDefinition.hashCode(), packageDefinition.hashCode());
        assertEquals(packageDefinition.hashCode(), packageDefinitionSameAttributes.hashCode());
        assertEquals(packageDefinition.hashCode(), packageDefinitionOtherResolutionOptional.hashCode());
        assertEquals(packageDefinition.hashCode(), packageDefinitionOtherVersion.hashCode());

        assumeDifferentHashCodes();
        assertNotEquals(packageDefinition.hashCode(), packageDefinitionOtherPackageName.hashCode());
    }

    @Test
    public void testToString() throws Exception {
        assertNotNull(packageDefinition.toString());
        assertTrue(packageDefinition.toString().contains("PackageDefinition"));
    }

    private static void assertContains(String string, String sequence) {
        assertTrue(format("String '%s' didn't contain wanted '%s'", string, sequence), string.contains(sequence));
    }

    private static void assertNotContains(String string, String sequence) {
        assertFalse(format("String '%s' did contain unwanted '%s'", string, sequence), string.contains(sequence));
    }
}
