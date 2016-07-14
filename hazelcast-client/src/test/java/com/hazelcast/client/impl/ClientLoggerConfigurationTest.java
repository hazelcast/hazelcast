package com.hazelcast.client.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class ClientLoggerConfigurationTest extends HazelcastTestSupport {

    private static TestHazelcastFactory hazelcastFactory;
    private static HazelcastInstance client;
    private final String RANDOM_STRING = randomString();
    private final String LOG_FILENAME = randomString();
    private File logFile;

    @After
    public void deleteLogFile () {
        System.clearProperty("hazelcast.logging.type");
        logFile = new File("./" + LOG_FILENAME);
        logFile.delete();
    }

    @Test
    public void testProgrammaticConfiguration() throws IOException {
        testLoggingWithConfiguration(true);
    }

    @Test
    public void testSystemPropertyConfiguration() throws IOException{
        testLoggingWithConfiguration(false);
    }

    // Test with programmatic or system property configuration according to boolean parameter.
    protected void testLoggingWithConfiguration(boolean programmaticConfiguration) throws IOException {

        hazelcastFactory = new TestHazelcastFactory();
        Config cg = new Config();
        cg.setProperty( "hazelcast.logging.type", "jdk" );
        hazelcastFactory.newHazelcastInstance(cg);

        //Setting log4j properties for logging to a file.
        Properties props = new Properties();
        props.setProperty("log4j.rootLogger" , "INFO, FILE");
        props.setProperty("log4j.appender.FILE" , "org.apache.log4j.FileAppender");
        props.setProperty("log4j.appender.FILE.layout" , "org.apache.log4j.PatternLayout");
        props.setProperty("log4j.appender.FILE.layout.ConversionPattern",RANDOM_STRING + "\n");
        props.setProperty("log4j.appender.FILE.File","./" + LOG_FILENAME);
        props.setProperty("log4j.appender.FILE.ImmediateFlush" , "true");
        PropertyConfigurator.configure(props);

        ClientConfig config = new ClientConfig() ;
        if (programmaticConfiguration) {
            config.setProperty( "hazelcast.logging.type", "log4j" );
        }
        else {
            System.setProperty( "hazelcast.logging.type", "log4j" );
        }
        client = hazelcastFactory.newHazelcastClient(config);
        client.shutdown();
        hazelcastFactory.shutdownAll();

        boolean matchFound = false;
        logFile = new File("./" + LOG_FILENAME);
        Scanner scanner = new Scanner(logFile);
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            if(line.contains(RANDOM_STRING)){
                matchFound = true;
                break;
            }
        }
        assertTrue(matchFound);
    }
}

