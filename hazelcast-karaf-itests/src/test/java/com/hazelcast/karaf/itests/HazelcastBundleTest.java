package com.hazelcast.karaf.itests;

import com.hazelcast.osgi.HazelcastOSGiService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.ConfigurationManager;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.karaf.options.LogLevelOption;
import org.ops4j.pax.exam.options.MavenArtifactUrlReference;
import org.ops4j.pax.exam.options.MavenUrlReference;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerClass;
import org.ops4j.pax.exam.util.Filter;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;

import javax.inject.Inject;
import java.io.File;

import static org.ops4j.pax.exam.CoreOptions.maven;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.configureConsole;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.configureSecurity;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.karafDistributionConfiguration;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.keepRuntimeFolder;
import static org.ops4j.pax.exam.karaf.options.KarafDistributionOption.logLevel;


@RunWith(PaxExam.class)
@ExamReactorStrategy(PerClass.class)
public class HazelcastBundleTest {

    @Inject
    protected BundleContext bundleContext;

    @Inject
    @Filter(timeout = 30000)
    protected HazelcastOSGiService hazelcastOSGiService;

    @Test
    public void testStartBundle() throws Exception {
        Bundle bundle = hazelcastOSGiService.getOwnerBundle();
        Assert.assertNotNull(bundle);
        Assert.assertEquals(Bundle.ACTIVE, bundle.getState());
    }

    @Configuration
    public Option[] config() {
        MavenArtifactUrlReference karafUrl = maven()
                .groupId("org.apache.karaf")
                .artifactId("apache-karaf")
                .versionAsInProject().type("tar.gz");

        MavenUrlReference karafStandardRepo = maven()
                .groupId("org.apache.karaf.features")
                .artifactId("standard")
                .version(karafVersion())
                .classifier("features")
                .type("xml");

        return new Option[]{
                karafDistributionConfiguration()
                        .frameworkUrl(karafUrl)
                        .name("Apache Karaf")
                        .unpackDirectory(new File("target/exam")),
                configureSecurity().disableKarafMBeanServerBuilder(),
                configureConsole().ignoreLocalConsole(),
                keepRuntimeFolder(),
                mavenBundle()
                        .groupId("org.apache.geronimo.specs")
                        .artifactId("geronimo-jta_1.1_spec")
                        .version("1.1.1").start(),
                mavenBundle()
                        .groupId("com.hazelcast")
                        .artifactId("hazelcast-all")
                        .versionAsInProject().start(),
                logLevel(LogLevelOption.LogLevel.INFO),
        };
    }

    public static String karafVersion() {
        ConfigurationManager cm = new ConfigurationManager();
        String karafVersion = cm.getProperty("pax.exam.karaf.version", "4.1.2");
        return karafVersion;
    }

}
