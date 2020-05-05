/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfigRecognizer;
import com.hazelcast.client.config.ClientFailoverConfigRecognizer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Scanner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigRecognizerTest {
    static final String HAZELCAST_START_TAG = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n";
    static final String HAZELCAST_END_TAG = "</hazelcast>\n";
    static final String HAZELCAST_CLIENT_START_TAG =
            "<hazelcast-client xmlns=\"http://www.hazelcast.com/schema/client-config\">\n";
    static final String HAZELCAST_CLIENT_END_TAG = "</hazelcast-client>";
    static final String HAZELCAST_CLIENT_FAILOVER_START_TAG =
            "<hazelcast-client-failover xmlns=\"http://www.hazelcast.com/schema/client-failover-config\">\n";
    static final String HAZELCAST_CLIENT_FAILOVER_END_TAG = "</hazelcast-client-failover>";

    @Test
    public void testRecognizeMemberFullBlownXml() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String xml = ""
                + "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<!--\n"
                + "BANNER\n"
                + "-->\n"
                + "\n"
                + HAZELCAST_START_TAG
                + "  <cluster-name>foobar</cluster-name>\n"
                + HAZELCAST_END_TAG;
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());

        assertTrue(memberRecognizer.isRecognized(bis));
        assertFalse(clientRecognizer.isRecognized(bis));
        assertFalse(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeMemberXml() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String xml = HAZELCAST_START_TAG
                + "  <cluster-name>foobar</cluster-name>\n"
                + HAZELCAST_END_TAG;
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());

        assertTrue(memberRecognizer.isRecognized(bis));
        assertFalse(clientRecognizer.isRecognized(bis));
        assertFalse(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeClientXml() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String xml = HAZELCAST_CLIENT_START_TAG
                + "  <cluster-name>foobar</cluster-name>\n"
                + HAZELCAST_CLIENT_END_TAG;
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());

        assertFalse(memberRecognizer.isRecognized(bis));
        assertTrue(clientRecognizer.isRecognized(bis));
        assertFalse(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeFailoverClientXml() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String xml = HAZELCAST_CLIENT_FAILOVER_START_TAG
                + "  <cluster-name>foobar</cluster-name>\n"
                + HAZELCAST_CLIENT_FAILOVER_END_TAG;
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());

        assertFalse(memberRecognizer.isRecognized(bis));
        assertFalse(clientRecognizer.isRecognized(bis));
        assertTrue(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testInvalidXmlIsNotRecognized() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String xml = "invalid-xml";
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());

        assertFalse(memberRecognizer.isRecognized(bis));
        assertFalse(clientRecognizer.isRecognized(bis));
        assertFalse(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeMemberYaml() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String yaml = ""
                + "hazelcast:\n"
                + "  cluster-name: foobar";
        ByteArrayInputStream bis = new ByteArrayInputStream(yaml.getBytes());

        assertTrue(memberRecognizer.isRecognized(bis));
        assertFalse(clientRecognizer.isRecognized(bis));
        assertFalse(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeClientYaml() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String yaml = ""
                + "hazelcast-client:\n"
                + "  cluster-name: foobar";
        ByteArrayInputStream bis = new ByteArrayInputStream(yaml.getBytes());

        assertFalse(memberRecognizer.isRecognized(bis));
        assertTrue(clientRecognizer.isRecognized(bis));
        assertFalse(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeFailoverClientYaml() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String yaml = ""
                + "hazelcast-client-failover:\n"
                + "  cluster-name: foobar";
        ByteArrayInputStream bis = new ByteArrayInputStream(yaml.getBytes());

        assertFalse(memberRecognizer.isRecognized(bis));
        assertFalse(clientRecognizer.isRecognized(bis));
        assertTrue(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testInvalidYamlIsNotRecognized() throws Exception {
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer();
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer();
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer();

        String yaml = "invalid-yaml";
        ByteArrayInputStream bis = new ByteArrayInputStream(yaml.getBytes());

        assertFalse(memberRecognizer.isRecognized(bis));
        assertFalse(clientRecognizer.isRecognized(bis));
        assertFalse(clientFailoverRecognizer.isRecognized(bis));
    }

    @Test
    public void testRecognizeWithCustomConfigRecognizer() throws Exception {
        ConfigRecognizer customRecognizer = new TestConfigRecognizer();
        ConfigRecognizer memberRecognizer = new MemberConfigRecognizer(customRecognizer);
        ConfigRecognizer clientRecognizer = new ClientConfigRecognizer(customRecognizer);
        ConfigRecognizer clientFailoverRecognizer = new ClientFailoverConfigRecognizer(customRecognizer);

        String config = "test-hazelcast-config";
        ByteArrayInputStream bis = new ByteArrayInputStream(config.getBytes());

        assertTrue(memberRecognizer.isRecognized(bis));
        assertTrue(clientRecognizer.isRecognized(bis));
        assertTrue(clientFailoverRecognizer.isRecognized(bis));
    }

    private class TestConfigRecognizer implements ConfigRecognizer {

        @Override
        public boolean isRecognized(InputStream configStream) throws Exception {
            String firstLine;
            try (Scanner scanner = new Scanner(configStream)) {
                firstLine = scanner.nextLine();
            }

            return "test-hazelcast-config".equalsIgnoreCase(firstLine);
        }
    }
}
