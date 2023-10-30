/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.osgi;

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.ExamReactorStrategy;
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

import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.options;

@Category(QuickTest.class)
@ExamReactorStrategy(AllConfinedStagedReactorFactory.class)
class HazelcastOSGiIT {

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

        String url = "reference:file:" + PathUtils.getBaseDir() + "/target/"
                + "hazelcast-" + BuildInfoProvider.getBuildInfo().getVersion() + ".jar";
        // modify url for Windows environment
        url = url.replace("\\", "/");
        UrlProvisionOption hzBundle = bundle(url);
        CompositeOption junitBundles = junitBundles();
        return options(hzBundle, junitBundles);
    }

    @AfterEach
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

    private HazelcastOSGiService getService() {
        ServiceReference serviceRef = bundleContext.getServiceReference(HazelcastOSGiService.class.getName());
        if (serviceRef == null) {
            return null;
        }
        return (HazelcastOSGiService) bundleContext.getService(serviceRef);
    }
}
