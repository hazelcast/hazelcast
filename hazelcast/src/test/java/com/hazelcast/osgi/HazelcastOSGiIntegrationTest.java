package com.hazelcast.osgi;

import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.ExamReactorStrategy;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;
import org.ops4j.pax.exam.options.CompositeOption;
import org.ops4j.pax.exam.options.UrlProvisionOption;
import org.ops4j.pax.exam.spi.reactors.AllConfinedStagedReactorFactory;
import org.ops4j.pax.exam.util.PathUtils;
import org.ops4j.pax.url.maven.commons.MavenConstants;
import org.ops4j.pax.url.mvn.ServiceConstants;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;

import javax.inject.Inject;

import static org.junit.Assert.assertNotNull;
import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.options;

@RunWith(JUnit4TestRunner.class)
@Category(QuickTest.class)
@ExamReactorStrategy(AllConfinedStagedReactorFactory.class)
public class HazelcastOSGiIntegrationTest {

    @Inject
    private BundleContext bundleContext;

    private static final String MAVEN_REPOSITORIES_PROP = ServiceConstants.PID + MavenConstants.PROPERTY_REPOSITORIES;
    private static final String MAVEN_REPOSITORIES = "https://osgi.sonatype.org/content/groups/pax-runner@id=paxrunner,"
                                                   + "https://repo1.maven.org/maven2@id=central";
    private String oldMavenRepoProperty;

    @Configuration
    public Option[] config() {
        oldMavenRepoProperty = System.getProperty(MAVEN_REPOSITORIES_PROP);
        System.setProperty(MAVEN_REPOSITORIES_PROP, MAVEN_REPOSITORIES);

        String url = "reference:file:" + PathUtils.getBaseDir() + "/hazelcast/target/classes";
        UrlProvisionOption hzBundle = bundle(url);
        CompositeOption junitBundles = junitBundles();
        return options(hzBundle, junitBundles);
    }

    @After
    public void tearDown() throws Exception {
        for (Bundle bundle : bundleContext.getBundles()) {
            if ("com.hazelcast".equals(bundle.getSymbolicName())) {
                bundle.uninstall();
                break;
            }
        }
        if (oldMavenRepoProperty == null) {
            System.clearProperty(MAVEN_REPOSITORIES_PROP);
        } else {
            System.setProperty(MAVEN_REPOSITORIES_PROP, oldMavenRepoProperty);
        }
    }

    @Test
    public void serviceRetrievedSuccessfully() {
        HazelcastOSGiService service = getService();
        assertNotNull(service);
    }

    private HazelcastOSGiService getService() {
        ServiceReference serviceRef = bundleContext.getServiceReference(HazelcastOSGiService.class.getName());
        if (serviceRef == null) {
            return null;
        }
        return (HazelcastOSGiService) bundleContext.getService(serviceRef);
    }
}
