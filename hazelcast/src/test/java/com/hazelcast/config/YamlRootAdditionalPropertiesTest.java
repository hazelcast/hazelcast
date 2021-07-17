package com.hazelcast.config;

import com.hazelcast.client.config.YamlClientConfigBuilderTest;
import com.hazelcast.internal.config.SchemaViolationConfigurationException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static com.hazelcast.config.YamlConfigBuilderTest.buildConfig;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class YamlRootAdditionalPropertiesTest {

    @Rule
    public ExpectedException expExc = ExpectedException.none();

    @Test
    public void testMisIndentedMemberConfigProperty_failsValidation() {
        SchemaViolationConfigurationException actual = assertThrows(SchemaViolationConfigurationException.class,
                () -> buildConfig("hazelcast:\n"
                        + "  instance-name: 'my-instance'\n"
                        + "cluster-name: 'my-cluster'\n")
                );
        assertEquals("Mis-indented hazelcast configuration property found: [cluster-name]", actual.getMessage());
    }

    @Test
    public void testMisIndented_NonConfigProperty_passes() {
        buildConfig("hazelcast:\n"
                + "  instance-name: 'my-instance'\n"
                + "other-prop: ''\n");
    }

    @Test
    public void testMisIndented_ClientConfigProperty_failsValidation() {
        SchemaViolationConfigurationException actual = assertThrows(SchemaViolationConfigurationException.class,
                () -> YamlClientConfigBuilderTest.buildConfig("hazelcast-client:\n"
                        + "  instance-name: 'my-instance'\n"
                        + "client-labels: 'my-lbl'\n")
        );
        assertEquals("Mis-indented hazelcast configuration property found: [client-labels]", actual.getMessage());
    }

    @Test
    public void multipleMisIndented_configProps() {
        SchemaViolationConfigurationException actual = assertThrows(SchemaViolationConfigurationException.class,
                () -> YamlClientConfigBuilderTest.buildConfig("hazelcast-client: {}\n"
                        + "instance-name: 'my-instance'\n"
                        + "client-labels: 'my-lbl'\n")
        );
        assertEquals(new SchemaViolationConfigurationException("2 schema violations found", "#", "#", asList(
                new SchemaViolationConfigurationException("Mis-indented hazelcast configuration property found: [instance-name]",
                        "#", "#", emptyList()),
                new SchemaViolationConfigurationException("Mis-indented hazelcast configuration property found: [client-labels]",
                        "#", "#", emptyList())
        )), actual);
    }
}
