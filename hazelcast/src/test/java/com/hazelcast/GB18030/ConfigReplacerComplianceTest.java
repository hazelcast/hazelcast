package com.hazelcast.GB18030;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.config.YamlClientConfigBuilder;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.AbstractConfigImportVariableReplacementTest;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.config.YamlConfigBuilder;
import com.hazelcast.config.helpers.DeclarativeConfigFileHelper;
import com.hazelcast.config.test.builders.ConfigReplacerBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigReplacerComplianceTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private DeclarativeConfigFileHelper helper;

    @Before
    public void setUp() {
        helper = new DeclarativeConfigFileHelper();
    }

    @After
    public void cleanup() {
        helper.ensureTestConfigDeleted();
        hazelcastFactory.terminateAll();
    }

    @Test
    public void test_xmlClientConfigReplacerCompliance() throws IOException {
        final String HAZELCAST_CLIENT_START_TAG =
                "<hazelcast-client xmlns=\"http://www.hazelcast.com/schema/client-config\">\n";
        final String HAZELCAST_CLIENT_END_TAG = "</hazelcast-client>";

        ConfigReplacerBuilder testReplacer = new ConfigReplacerBuilder()
                .withClass(AbstractConfigImportVariableReplacementTest.TestReplacer.class)
                .addProperty("撒尿", "${撒尿}")
                .addProperty("也撒尿", "")
                .addProperty("小便三", "另一个财产")
                .addProperty("小便四", "&lt;测试/&gt;");
        String configReplacer = HAZELCAST_CLIENT_START_TAG
                + "    <config-replacers fail-if-value-missing='false'>\n"
                + testReplacer.build()
                + "    </config-replacers>\n"
                + HAZELCAST_CLIENT_END_TAG;
        String configReplacerLocation = helper.givenConfigFileInWorkDir("config-replacer.xml", configReplacer).getAbsolutePath();

        String clusterName = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"" + "file:///" + configReplacerLocation + "\"/>\n"
                + "    <cluster-name>$T{撒尿} $T{也撒尿} $T{小便三} $T{小便四} $T{小便五}</cluster-name>\n"
                + HAZELCAST_CLIENT_END_TAG;

        String clusterNameLocation = helper.givenConfigFileInWorkDir("cluster-name.xml", clusterName).getAbsolutePath();

        String xml = HAZELCAST_CLIENT_START_TAG
                + "    <import resource=\"${config.location}\"/>\n"
                + HAZELCAST_CLIENT_END_TAG;

        Properties properties = new Properties(System.getProperties());
        properties.put("config.location", clusterNameLocation);
        properties.put("撒尿", "财产");
        ClientConfig config = buildClientXmlConfig(xml, properties);
        assertEquals("财产  另一个财产 <测试/> $T{小便五}", config.getClusterName());
    }

    @Test
    public void test_xmlConfigReplacerCompliance() {
        final String HAZELCAST_START_TAG = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n";
        final String HAZELCAST_END_TAG = "</hazelcast>\n";

        ConfigReplacerBuilder testReplacer = new ConfigReplacerBuilder()
                .withClass(AbstractConfigImportVariableReplacementTest.TestReplacer.class)
                .addProperty("撒尿", "财产")
                .addProperty("也撒尿", "")
                .addProperty("小便三", "另一个财产")
                .addProperty("小便四", "&lt;测试/&gt;");
        String xml = HAZELCAST_START_TAG
                + "    <config-replacers fail-if-value-missing='false'>\n"
                + testReplacer.build()
                + "    </config-replacers>\n"
                + "    <cluster-name>$T{撒尿} $T{也撒尿} $T{小便三} $T{小便四} $T{p5}</cluster-name>\n"
                + HAZELCAST_END_TAG;
        Config config = buildXmlConfig(xml, System.getProperties());
        assertEquals("财产  另一个财产 <测试/> $T{p5}", config.getClusterName());
    }

    @Test
    public void test_yamlConfigReplacerCompliance() throws IOException {
        String configReplacer = ""
                + "hazelcast:\n"
                + "  config-replacers:\n"
                + "    fail-if-value-missing: false\n"
                + "    replacers:\n"
                + "      - class-name: " + AbstractConfigImportVariableReplacementTest.TestReplacer.class.getName() + "\n"
                + "        properties:\n"
                + "          撒尿: ${撒尿}\n"
                + "          也撒尿: \"\"\n"
                + "          小便三: 另一个财产\n"
                + "          小便四: <测试/>\n";

        String configReplacerLocation = helper.givenConfigFileInWorkDir("config-replacer.yaml", configReplacer).getAbsolutePath();

        String clusterName = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - " + configReplacerLocation + "\n"
                + "  cluster-name: $T{撒尿} $T{也撒尿} $T{小便三} $T{小便四} $T{p5}\n";

        String clusterNameLocation = helper.givenConfigFileInWorkDir("cluster-name.yaml", clusterName).getAbsolutePath();

        String yaml = ""
                + "hazelcast:\n"
                + "  import:\n"
                + "    - " + "${config.location}\n";

        Properties properties = new Properties(System.getProperties());
        properties.put("config.location", clusterNameLocation);
        properties.put("撒尿", "财产");
        Config config = buildYamlConfig(yaml, properties);
        assertEquals("财产  另一个财产 <测试/> $T{p5}", config.getClusterName());
    }

    @Test
    public void testReplacerProperties() {
        String yaml = ""
                + "hazelcast-client:\n"
                + "  config-replacers:\n"
                + "    fail-if-value-missing: false\n"
                + "    replacers:\n"
                + "      - class-name: " + AbstractConfigImportVariableReplacementTest.TestReplacer.class.getName() + "\n"
                + "        properties:\n"
                + "          p1: a property\n"
                + "          p2: \"\"\n"
                + "          p3: another property\n"
                + "          p4: <test/>\n"
                + "  cluster-name: $T{p1} $T{p2} $T{p3} $T{p4} $T{p5}";
        ClientConfig config = buildClientYamlConfig(yaml, System.getProperties());
        assertEquals("a property  another property <test/> $T{p5}", config.getClusterName());
    }

    static ClientConfig buildClientXmlConfig(String xml, Properties properties) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder(bis);
        configBuilder.setProperties(properties);
        return configBuilder.build();
    }

    static Config buildXmlConfig(String xml, Properties properties) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        configBuilder.setProperties(properties);
        return configBuilder.build();
    }

    static Config buildYamlConfig(String yaml, Properties properties) {
        ByteArrayInputStream bis = new ByteArrayInputStream(yaml.getBytes());
        YamlConfigBuilder configBuilder = new YamlConfigBuilder(bis);
        configBuilder.setProperties(properties);
        return configBuilder.build();
    }

    static ClientConfig buildClientYamlConfig(String xml, Properties properties) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        YamlClientConfigBuilder configBuilder = new YamlClientConfigBuilder(bis);
        configBuilder.setProperties(properties);
        return configBuilder.build();
    }
}
