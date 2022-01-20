package com.hazelcast.internal.config.dynamic;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigXmlGenerator;
import com.hazelcast.config.InMemoryXmlConfig;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.config.dynamic.DynamicConfigXmlGenerator.licenseKeyXmlGenerator;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;

// Serial because there is one test, make parallel otherwise
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigXmlGeneratorTest {

    private static final ILogger LOGGER = Logger.getLogger(DynamicConfigXmlGeneratorTest.class);

    // LICENSE KEY

    @Test
    public void testLicenseKey() {
        String licenseKey = randomString();
        Config config = new Config().setLicenseKey(licenseKey);

        StringBuilder xmlBuilder = new StringBuilder();
        ConfigXmlGenerator.XmlGenerator gen = new ConfigXmlGenerator.XmlGenerator(xmlBuilder);

        // generate string
        xmlBuilder.append("<hazelcast ")
                .append("xmlns=\"http://www.hazelcast.com/schema/config\"\n")
                .append("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n")
                .append("xsi:schemaLocation=\"http://www.hazelcast.com/schema/config ")
                .append("http://www.hazelcast.com/schema/config/hazelcast-config-")
                .append(Versions.CURRENT_CLUSTER_VERSION.getMajor())
                .append('.')
                .append(Versions.CURRENT_CLUSTER_VERSION.getMinor())
                .append(".xsd\">");
        licenseKeyXmlGenerator(gen, config);
        xmlBuilder.append("</hazelcast>");

        // recreate the config object
        String xmlString = xmlBuilder.toString();
        Config decConfig = new InMemoryXmlConfig(xmlString);

        // test
        String actualLicenseKey = decConfig.getLicenseKey();
        assertEquals(config.getLicenseKey(), actualLicenseKey);
    }
}
