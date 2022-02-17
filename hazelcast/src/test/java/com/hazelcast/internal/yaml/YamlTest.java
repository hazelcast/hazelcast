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

import com.google.common.io.CharStreams;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static com.hazelcast.internal.yaml.YamlUtil.asMapping;
import static com.hazelcast.internal.yaml.YamlUtil.asSequence;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class YamlTest {
    private static final int NOT_EXISTING = 42;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testYamlFromInputStream() {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-root-map.yaml");
        YamlNode root = YamlLoader.load(inputStream, "root-map");
        verify(root);
    }

    @Test
    public void testYamlFromInputStreamWithoutRootName() {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-root-map.yaml");
        YamlNode root = YamlLoader.load(inputStream);
        verify(asMapping(root).childAsMapping("root-map"));
    }

    @Test
    public void testYamlExtendedTestFromInputStream() {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-root-map-extended.yaml");
        YamlNode root = YamlLoader.load(inputStream, "root-map");
        verify(root);
        verifyExtendedYaml(root);
    }

    @Test
    public void testJsonFromInputStream() {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-root-map.json");
        YamlNode root = YamlLoader.load(inputStream, "root-map");
        verify(root);
    }

    @Test
    public void testYamlFromReader() {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-root-map.yaml");
        InputStreamReader reader = new InputStreamReader(inputStream);
        YamlNode root = YamlLoader.load(reader, "root-map");
        verify(root);
    }

    @Test
    public void testYamlFromReaderWithoutRootName() {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-root-map.yaml");
        InputStreamReader reader = new InputStreamReader(inputStream);
        YamlNode root = YamlLoader.load(reader);
        verify(asMapping(root).childAsMapping("root-map"));
    }

    @Test
    public void testYamlFromString() throws IOException {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-root-map.yaml");
        InputStreamReader reader = new InputStreamReader(inputStream);
        String yamlString = CharStreams.toString(reader);
        YamlNode root = YamlLoader.load(yamlString, "root-map");
        verify(root);
    }

    @Test
    public void testYamlFromStringWithoutRootMap() throws IOException {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-root-map.yaml");
        InputStreamReader reader = new InputStreamReader(inputStream);
        String yamlString = CharStreams.toString(reader);
        YamlNode root = YamlLoader.load(yamlString);
        verify(asMapping(root).childAsMapping("root-map"));
    }

    @Test
    public void testLoadingInvalidYamlFromInputStream() {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-invalid.yaml");
        expectedException.expect(YamlException.class);
        YamlLoader.load(inputStream);
    }

    @Test
    public void testLoadingInvalidYamlFromInputStreamWithRootName() {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-invalid.yaml");
        expectedException.expect(YamlException.class);
        YamlLoader.load(inputStream, "root-map");
    }

    @Test
    public void testLoadingInvalidYamlFromReader() {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-invalid.yaml");
        InputStreamReader reader = new InputStreamReader(inputStream);
        expectedException.expect(YamlException.class);
        YamlLoader.load(reader);
    }

    @Test
    public void testLoadingInvalidYamlFromReaderWithRootName() {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-invalid.yaml");
        InputStreamReader reader = new InputStreamReader(inputStream);
        expectedException.expect(YamlException.class);
        YamlLoader.load(reader, "root-map");
    }

    @Test
    public void testLoadingInvalidYamlFromString() throws IOException {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-invalid.yaml");
        InputStreamReader reader = new InputStreamReader(inputStream);
        String yamlString = CharStreams.toString(reader);
        expectedException.expect(YamlException.class);
        YamlLoader.load(yamlString);
    }

    @Test
    public void testLoadingInvalidYamlFromStringWithRootName() throws IOException {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-invalid.yaml");
        InputStreamReader reader = new InputStreamReader(inputStream);
        String yamlString = CharStreams.toString(reader);
        expectedException.expect(YamlException.class);
        YamlLoader.load(yamlString, "root-map");
    }

    @Test
    public void testInvalidScalarValueTypeMap() {
        YamlMapping rootMap = getYamlRoot();
        YamlMapping embeddedMap = rootMap.childAsMapping("embedded-map");

        expectedException.expect(ClassCastException.class);
        int notAnInt = embeddedMap.childAsScalarValue("scalar-str");
    }

    @Test
    public void testInvalidScalarValueTypeSeq() {
        YamlMapping rootMap = getYamlRoot();
        YamlSequence embeddedList = rootMap
                .childAsMapping("embedded-map")
                .childAsSequence("embedded-list");

        expectedException.expect(ClassCastException.class);
        int notAnInt = embeddedList.childAsScalarValue(0);
    }

    @Test
    public void testInvalidScalarValueTypeHintedMap() {
        YamlMapping rootMap = getYamlRoot();
        YamlMapping embeddedMap = rootMap.childAsMapping("embedded-map");

        embeddedMap.childAsScalarValue("scalar-str", String.class);

        expectedException.expect(YamlException.class);
        embeddedMap.childAsScalarValue("scalar-str", Integer.class);
    }

    @Test
    public void testInvalidScalarValueTypeHintedSeq() {
        YamlMapping rootMap = getYamlRoot();
        YamlSequence embeddedList = rootMap
                .childAsMapping("embedded-map")
                .childAsSequence("embedded-list");

        embeddedList.childAsScalarValue(0, String.class);

        expectedException.expect(YamlException.class);
        embeddedList.childAsScalarValue(0, Integer.class);
    }

    @Test
    public void testNotExistingMappingFromMap() {
        assertNull(getYamlRoot().childAsMapping("not-existing"));
    }

    @Test
    public void testNotExistingSequenceFromMap() {
        assertNull(getYamlRoot().childAsSequence("not-existing"));
    }

    @Test
    public void testNotExistingScalarFromMap() {
        assertNull(getYamlRoot().childAsScalar("not-existing"));
    }

    @Test
    public void testNotExistingMappingFromSeq() {
        YamlSequence seq = getYamlRoot()
                .childAsMapping("embedded-map")
                .childAsSequence("embedded-list");
        assertNull(seq.childAsMapping(NOT_EXISTING));
    }

    @Test
    public void testNotExistingSequenceFromSeq() {
        YamlSequence seq = getYamlRoot()
                .childAsMapping("embedded-map")
                .childAsSequence("embedded-list");
        assertNull(seq.childAsSequence(NOT_EXISTING));
    }

    @Test
    public void testNotExistingScalarFromSeq() {
        YamlSequence seq = getYamlRoot()
                .childAsMapping("embedded-map")
                .childAsSequence("embedded-list");
        assertNull(seq.childAsScalar(NOT_EXISTING));
    }

    @Test
    public void testInvalidNodeTypeNotAMapping() {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-root-map.yaml");
        YamlNode root = YamlLoader.load(inputStream, "root-map");

        YamlMapping embeddedMap = ((YamlMapping) root)
                .childAsMapping("embedded-map");

        expectedException.expect(YamlException.class);
        embeddedMap.childAsMapping("embedded-list");
    }

    @Test
    public void testInvalidNodeTypeNotASeq() {
        YamlMapping rootMap = getYamlRoot();

        expectedException.expect(YamlException.class);
        rootMap.childAsSequence("embedded-map");
    }

    @Test
    public void testInvalidNodeTypeNotAScalar() {
        YamlMapping rootMap = getYamlRoot();

        expectedException.expect(YamlException.class);
        rootMap.childAsScalar("embedded-map");
    }

    @Test
    public void testIterateChildrenMap() {
        YamlMapping embeddedMap = getYamlRoot()
                .childAsMapping("embedded-map");

        int childCount = 0;
        for (YamlNode node : embeddedMap.children()) {
            assertNotNull(node);
            childCount++;
        }

        assertEquals(6, childCount);
    }

    @Test
    public void testIterateChildrenSeq() {
        YamlSequence embeddedList = getYamlRoot()
                .childAsMapping("embedded-map")
                .childAsSequence("embedded-list");

        int childCount = 0;
        for (YamlNode node : embeddedList.children()) {
            assertNotNull(node);
            childCount++;
        }

        assertEquals(4, childCount);
    }

    @Test
    public void testParentOfRootIsNull() {
        assertNull(getYamlRoot().parent());
    }

    @Test
    public void testParentOfEmbeddedMapIsRoot() {
        YamlMapping root = getYamlRoot();
        assertSame(root, root.childAsMapping("embedded-map").parent());
    }

    @Test
    public void testParentOfScalarIntIsEmbeddedMap() {
        YamlMapping embeddedMap = getYamlRoot().childAsMapping("embedded-map");
        assertSame(embeddedMap, embeddedMap.childAsScalar("scalar-int").parent());
    }

    @Test
    public void testNameOfMap() {
        assertEquals("embedded-map", getYamlRoot().childAsMapping("embedded-map").nodeName());
    }

    @Test
    public void testNameOfSeq() {
        assertEquals("embedded-list", getYamlRoot().childAsMapping("embedded-map")
                                                   .childAsSequence("embedded-list")
                                                   .nodeName());
    }

    @Test
    public void testNameOfNamedScalar() {
        assertEquals("scalar-int", getYamlRoot().childAsMapping("embedded-map")
                                                .childAsScalar("scalar-int")
                                                .nodeName());
    }

    @Test
    public void testNameOfUnnamedScalar() {
        assertSame(YamlNode.UNNAMED_NODE, getYamlRoot().childAsMapping("embedded-map")
                                                       .childAsSequence("embedded-list")
                                                       .childAsScalar(0)
                                                       .nodeName());
    }

    @Test
    public void testYamlListInRoot() throws IOException {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-root-seq.yaml");
        InputStreamReader reader = new InputStreamReader(inputStream);
        String yamlString = CharStreams.toString(reader);
        YamlNode root = YamlLoader.load(yamlString);

        assertTrue(root instanceof YamlSequence);

        YamlSequence rootSeq = asSequence(root);
        assertEquals(42, ((Integer) rootSeq.childAsScalarValue(0)).intValue());

        YamlMapping map = rootSeq.childAsMapping(1);
        assertEquals(YamlNode.UNNAMED_NODE, map.nodeName());
        assertEquals("embedded-map", map.childAsMapping("embedded-map").nodeName());
    }

    private void verify(YamlNode root) {
        assertTrue(root instanceof YamlMapping);

        YamlMapping rootMap = (YamlMapping) root;
        YamlMapping embeddedMap = rootMap.childAsMapping("embedded-map");
        String scalarString = embeddedMap.childAsScalarValue("scalar-str");
        int scalarInt = embeddedMap.childAsScalarValue("scalar-int");
        double scalarDouble = embeddedMap.childAsScalarValue("scalar-double");
        boolean scalarBool = embeddedMap.childAsScalarValue("scalar-bool");

        YamlSequence embeddedList = embeddedMap.childAsSequence("embedded-list");
        String elItem0 = embeddedList.childAsScalarValue(0);
        YamlScalar elItem0AsScalar = embeddedList.childAsScalar(0);
        int elItem1 = embeddedList.childAsScalarValue(1);
        double elItem2 = embeddedList.childAsScalarValue(2);
        boolean elItem3 = embeddedList.childAsScalarValue(3);

        YamlSequence embeddedList2 = embeddedMap.childAsSequence("embedded-list2");
        String el2Item0 = embeddedList2.childAsScalarValue(0);
        double el2Item1 = embeddedList2.childAsScalarValue(1);

        assertEquals("embedded-map", embeddedMap.nodeName());
        assertEquals("embedded-list", embeddedList.nodeName());

        // root-map/embedded-map/scalars
        assertEquals(6, embeddedMap.childCount());
        assertEquals("h4z3lc4st", scalarString);
        assertEquals(123, scalarInt);
        assertEquals(123.12312D, scalarDouble, 10E-5);
        assertTrue(scalarBool);

        // root-map/embedded-map/embedded-list
        assertEquals("value1", elItem0);
        assertTrue(elItem0AsScalar.isA(String.class));
        assertEquals("value1", elItem0AsScalar.nodeValue());
        assertEquals(NOT_EXISTING, elItem1);
        assertEquals(42.42D, elItem2, 10E-2);
        assertFalse(elItem3);

        // root-map/embedded-map/embedded-list2
        assertEquals(2, embeddedList2.childCount());
        assertEquals("value2", el2Item0);
        assertEquals(1D, el2Item1, 10E-1);
    }

    /*
     * Verifies can't be tested in YAML and JSON together because JSON
     * doesn't support everything that YAML does, like
     * - Embedded mapping in sequences
     * - Multiline strings
     */
    private void verifyExtendedYaml(YamlNode root) {
        String keysValue = ((YamlMapping) root)
                .childAsMapping("embedded-map")
                .childAsSequence("embedded-list")
                .childAsMapping(4)
                .childAsScalarValue("key");
        assertEquals("value", keysValue);

        String multilineStr = ((YamlMapping) root).childAsScalarValue("multiline-str");
        assertEquals("Hazelcast IMDG\n"
                        + "The Leading Open Source In-Memory Data Grid:\n"
                        + "Distributed Computing, Simplified.\n",
                multilineStr);
    }

    private YamlMapping getYamlRoot() {
        InputStream inputStream = YamlTest.class.getClassLoader().getResourceAsStream("yaml-test-root-map.yaml");
        YamlNode root = YamlLoader.load(inputStream, "root-map");

        return (YamlMapping) root;
    }
}
