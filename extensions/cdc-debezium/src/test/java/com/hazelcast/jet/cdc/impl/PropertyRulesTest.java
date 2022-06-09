package com.hazelcast.jet.cdc.impl;

import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static org.junit.Assert.*;

@Category(QuickTest.class)
public class PropertyRulesTest {

    @Test
    public void requiredCheck() {
        PropertyRules rules = new PropertyRules().required("abc");
        Properties properties = new Properties();

        assertFail(rules, properties, "abc must be specified");

        properties.setProperty("abc", "test");
        rules.check(properties);
    }

    @Test
    public void requiredExcludedIncludedCombinedCheck() {
        PropertyRules rules = new PropertyRules()
                .required("abc")
                .exclusive("exclusive", "other")
                .inclusive("inclusive", "totallyOther");
        Properties properties = new Properties();
        properties.setProperty("exclusive", "test");
        properties.setProperty("other", "test");
        properties.setProperty("inclusive", "test");

        assertFail(rules, properties, "abc must be specified, exclusive and other are mutually exclusive, " +
                "inclusive requires totallyOther to be set too");
    }

    private void assertFail(PropertyRules rules, Properties properties, String messageExpected) {
        try {
            rules.check(properties);
            fail("IllegalStateException expected for rules " + rules + " and properties " + properties);
        } catch (IllegalStateException expected) {
            assertEquals(messageExpected, expected.getMessage());
        }
    }

}