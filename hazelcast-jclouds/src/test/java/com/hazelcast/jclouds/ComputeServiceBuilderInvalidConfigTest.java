package com.hazelcast.jclouds;


import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.util.HashMap;

import static com.hazelcast.util.StringUtil.stringToBytes;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ComputeServiceBuilderInvalidConfigTest extends HazelcastTestSupport {

    public static final String configTemplate = "<hazelcast xsi:schemaLocation=\"http://www.hazelcast.com/schema/config hazelcast-config-3.6.xsd\"" +
            "           xmlns=\"http://www.hazelcast.com/schema/config\"" +
            "           xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">" +
            "    <properties>" +
            "        <property name=\"hazelcast.discovery.enabled\">true</property>" +
            "    </properties>" +
            "    <network>" +
            "        <join>" +
            "            <multicast enabled=\"false\"/>" +
            "            <tcp-ip enabled=\"false\" />" +
            "            <aws enabled=\"false\"/>" +
            "            <discovery-strategies>" +
            "                <discovery-strategy enabled=\"true\" class=\"com.hazelcast.jclouds.JCloudsDiscoveryStrategy\">" +
            "                    <properties>" +
                                 "${PROPERTIES_PLACE_HOLDER}"+
            "                    </properties>" +
            "                </discovery-strategy>" +
            "            </discovery-strategies>" +
            "        </join>" +
            "    </network>" +
            "</hazelcast>";

    @After
    public void cleanup() {
        Hazelcast.shutdownAll();
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_invalid_config_when_no_provider_is_set() throws Exception {

        String propertiesUnderTest = "<property name=\"identity\">test</property>" +
                                     "<property name=\"credential\">test</property>";

        String configXML = configTemplate.replace("${PROPERTIES_PLACE_HOLDER}", propertiesUnderTest);
        Config config = new XmlConfigBuilder(new ByteArrayInputStream(stringToBytes(configXML))).build();
        Hazelcast.newHazelcastInstance(config);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_invalid_config_when_credential_and_credential_path_set_together() throws Exception {

        String propertiesUnderTest = "<property name=\"provider\">test</property>" +
                                     "<property name=\"identity\">test</property>" +
                                     "<property name=\"credential\">test</property>" +
                                     "<property name=\"credentialPath\">test</property>";

        String configXML = configTemplate.replace("${PROPERTIES_PLACE_HOLDER}", propertiesUnderTest);
        Config config = new XmlConfigBuilder(new ByteArrayInputStream(stringToBytes(configXML))).build();
        Hazelcast.newHazelcastInstance(config);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void test_invalid_config_when_both_IAM_and_credential_set() throws Exception {

        String propertiesUnderTest = "<property name=\"provider\">aws-ec2</property>" +
                                     "<property name=\"role-name\">test</property>" +
                                     "<property name=\"identity\">test</property>" +
                                     "<property name=\"credential\">test</property>";

        String configXML = configTemplate.replace("${PROPERTIES_PLACE_HOLDER}", propertiesUnderTest);
        Config config = new XmlConfigBuilder(new ByteArrayInputStream(stringToBytes(configXML))).build();
        Hazelcast.newHazelcastInstance(config);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void test_invalid_config_when_IAM_role_is_configured_with_nonEc2_Provider() throws Exception {

        String propertiesUnderTest = "<property name=\"provider\">google-compute-engine</property>" +
                                      "<property name=\"role-name\">test</property>";

        String configXML = configTemplate.replace("${PROPERTIES_PLACE_HOLDER}", propertiesUnderTest);
        Config config = new XmlConfigBuilder(new ByteArrayInputStream(stringToBytes(configXML))).build();
        Hazelcast.newHazelcastInstance(config);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void test_invalid_config_when_tag_keys_and_values_have_different_sizes() throws Exception {

        String propertiesUnderTest = "<property name=\"provider\">aws-ec2</property>" +
                                     "<property name=\"identity\">test</property>" +
                                     "<property name=\"credential\">test</property>" +
                                     "<property name=\"tag-keys\">tag1,tag2,tag3</property>" +
                                     "<property name=\"tag-values\">tag1,tag2</property>";


        String configXML = configTemplate.replace("${PROPERTIES_PLACE_HOLDER}", propertiesUnderTest);
        Config config = new XmlConfigBuilder(new ByteArrayInputStream(stringToBytes(configXML))).build();
        Hazelcast.newHazelcastInstance(config);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void test_invalid_config_when_port_config_exceed_max_value() throws Exception {

        String propertiesUnderTest = "<property name=\"provider\">aws-ec2</property>" +
                "<property name=\"identity\">test</property>" +
                "<property name=\"credential\">test</property>" +
                "<property name=\"hz-port\">78000</property>";

        String configXML = configTemplate.replace("${PROPERTIES_PLACE_HOLDER}", propertiesUnderTest);
        Config config = new XmlConfigBuilder(new ByteArrayInputStream(stringToBytes(configXML))).build();
        Hazelcast.newHazelcastInstance(config);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void test_invalid_config_when_port_config_smaller_than_min_value() throws Exception {

        String propertiesUnderTest = "<property name=\"provider\">aws-ec2</property>" +
                "<property name=\"identity\">test</property>" +
                "<property name=\"credential\">test</property>" +
                "<property name=\"hz-port\">-1</property>";

        String configXML = configTemplate.replace("${PROPERTIES_PLACE_HOLDER}", propertiesUnderTest);
        Config config = new XmlConfigBuilder(new ByteArrayInputStream(stringToBytes(configXML))).build();
        Hazelcast.newHazelcastInstance(config);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void test_getCredentialFromFile_when_IOException() throws Exception {
        ComputeServiceBuilder builder = new ComputeServiceBuilder(new HashMap<String, Comparable>());
        builder.getCredentialFromFile("google-compute-engine", "blahblah.json");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void test_InvalidCloudProvider() throws Exception {
        ComputeServiceBuilder builder = new ComputeServiceBuilder(new HashMap<String, Comparable>());
        builder.newContextBuilder("invalid-cloud-provider", "identity", "credential", null);
    }
}
