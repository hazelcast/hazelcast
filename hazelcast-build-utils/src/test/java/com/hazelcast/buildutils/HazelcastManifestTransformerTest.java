package com.hazelcast.buildutils;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
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
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.nio.IOUtil.getFileFromResources;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyByte;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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

        transformer.mainClass = "com.hazelcast.core.server.StartServer";
        transformer.manifestEntries = new HashMap<String, Attributes>();
        transformer.overrideInstructions = new HashMap<String, String>();
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
    @SuppressWarnings("Since15")
    public void testTransformation() throws Exception {
        transformer.processResource(null, is, Collections.<Relocator>emptyList());
        transformer.modifyOutputStream(os);

        verify(os).putNextEntry(any(JarEntry.class));
        verify(os, atLeastOnce()).write(anyByte());
        verify(os, atLeastOnce()).flush();
        verifyNoMoreInteractions(os);
    }
}
