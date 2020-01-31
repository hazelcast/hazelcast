package com.hazelcast.config;

import com.hazelcast.log.encoders.StringEncoder;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LogConfigTest extends HazelcastTestSupport {


    @Test
    public void whenXmlConfig() {
        Config config = new InMemoryXmlConfig("<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n"
                + "<log name=\"foo\">\n"
                + "   <backup-count>2</backup-count>\n"
                + "   <async-backup-count>3</async-backup-count>\n"
                + "   <segment-size>1024</segment-size>\n"
                + "   <max-segment-count>64</max-segment-count>\n"
                + "   <retention-millis>60000</retention-millis>\n"
                + "   <tenuring-age-millis>60</tenuring-age-millis>\n"
                + "   <type>java.lang.String</type>\n"
                + "   <encoder>com.hazelcast.log.encoders.StringEncoder</encoder>\n"
                + "</log>\n"
                + "</hazelcast>");

        LogConfig logConfig = config.getLogConfig("foo");

        assertNotNull(logConfig);
        assertEquals("foo", logConfig.getName());
        assertEquals(2, logConfig.getBackupCount());
        assertEquals(3, logConfig.getAsyncBackupCount());
        assertEquals(1024, logConfig.getSegmentSize());
        assertEquals(64,logConfig.getMaxSegmentCount());
        assertEquals(60000,logConfig.getRetentionMillis());
        assertEquals(60, logConfig.getTenuringAgeMillis());
        assertEquals("java.lang.String", logConfig.getType());
        assertInstanceOf(StringEncoder.class, logConfig.getEncoder());
    }
}
