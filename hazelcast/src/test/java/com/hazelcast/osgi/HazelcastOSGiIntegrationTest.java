package com.hazelcast.osgi;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.ExamReactorStrategy;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;
import org.ops4j.pax.exam.spi.reactors.AllConfinedStagedReactorFactory;
import org.ops4j.pax.exam.util.PathUtils;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;

import javax.inject.Inject;

import static org.junit.Assert.assertNotNull;
import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.options;

@RunWith(JUnit4TestRunner.class)
@ExamReactorStrategy(AllConfinedStagedReactorFactory.class)
public class HazelcastOSGiIntegrationTest {

    @Inject
    private BundleContext bundleContext;

    @Configuration
    public Option[] config() {
        return options(bundle("reference:file:" + PathUtils.getBaseDir() + "/hazelcast/target/classes"),
                       junitBundles());
    }

    @After
    public void tearDown() throws BundleException {
        for (Bundle bundle : bundleContext.getBundles()) {
            if ("com.hazelcast".equals(bundle.getSymbolicName())) {
                bundle.uninstall();
                break;
            }
        }
    }

    private HazelcastOSGiService getService() {
        ServiceReference serviceRef = bundleContext.getServiceReference(HazelcastOSGiService.class.getName());
        if (serviceRef == null) {
            return null;
        }
        return (HazelcastOSGiService) bundleContext.getService(serviceRef);
    }

    @Test
    public void serviceRetrievedSuccessfully() throws InvalidSyntaxException {
        HazelcastOSGiService service = getService();
        assertNotNull(service);
    }

}
