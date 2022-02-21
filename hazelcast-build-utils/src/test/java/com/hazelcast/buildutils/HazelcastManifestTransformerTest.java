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

import com.hazelcast.internal.util.JavaVersion;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.maven.plugins.shade.relocation.Relocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.nio.IOUtil.getFileFromResources;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastManifestTransformerTest {

    private final File manifestFile = getFileFromResources("manifest.mf");

    private InputStream is;
    private JarOutputStream os;

    private HazelcastManifestTransformer transformer;

    @Before
    public void setUp() throws Exception {
        is = new FileInputStream(manifestFile);
        os = mock(JarOutputStream.class);

        transformer = new HazelcastManifestTransformer();

        transformer.setMainClass("com.hazelcast.core.server.HazelcastMemberStarter");
        transformer.setManifestEntries(new HashMap<String, Object>());
        transformer.setOverrideInstructions(new HashMap<String, String>());
    }

    @After
    public void tearDown() {
        closeResource(is);
    }

    @Test
    public void testCanTransformResource() {
        assertTrue(transformer.canTransformResource("META-INF/MANIFEST.MF"));
        assertTrue(transformer.canTransformResource("META-INF/manifest.mf"));

        assertFalse(transformer.canTransformResource("MANIFEST.MF"));
        assertFalse(transformer.canTransformResource("manifest.mf"));
    }

    @Test
    public void testHasTransformedResource() {
        assertTrue(transformer.hasTransformedResource());
    }

    @Test
    public void testTransformation() throws Exception {
        transformer.processResource(null, is, Collections.<Relocator>emptyList());
        transformer.modifyOutputStream(os);

        verify(os).putNextEntry(any(JarEntry.class));
        verify(os, atLeastOnce()).write(anyInt());
        verify(os, atLeastOnce()).flush();
        if (JavaVersion.isAtLeast(JavaVersion.JAVA_13)) {
            verify(os, atLeastOnce()).write(any(byte[].class), anyInt(), anyInt());
        }
        verifyNoMoreInteractions(os);
    }
}
