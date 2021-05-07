package com.hazelcast.config;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.XMLConfigBuilderTest.HAZELCAST_END_TAG;
import static com.hazelcast.config.XMLConfigBuilderTest.HAZELCAST_START_TAG;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SchemaValidationSkippingTest {

    @Rule
    public OverridePropertyRule skipProp = OverridePropertyRule.clear("hazelcast.config.schema.validation.enabled");

    @Test
    public void testSkippingYamlSchemaValidation() {
        System.setProperty("hazelcast.config.schema.validation.enabled", "false");
        String yaml = "invalid: yes";
        YamlConfigBuilderTest.buildConfig(yaml);
    }

    @Test
    public void testSkippingXmlSchemaValidation() {
        System.setProperty("hazelcast.config.schema.validation.enabled", "false");
        String xml = HAZELCAST_START_TAG + "<invalid></invalid>" + HAZELCAST_END_TAG;
        XMLConfigBuilderTest.buildConfig(xml);
    }

}
