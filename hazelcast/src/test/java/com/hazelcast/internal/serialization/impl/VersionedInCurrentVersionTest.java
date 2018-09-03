/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.GuardianException;
import com.hazelcast.test.starter.HazelcastVersionLocator;
import com.hazelcast.util.StringUtil;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ConfigurationBuilder;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.hazelcast.instance.BuildInfoProvider.getBuildInfo;
import static com.hazelcast.internal.cluster.Versions.CURRENT_CLUSTER_VERSION;
import static com.hazelcast.internal.cluster.Versions.PREVIOUS_CLUSTER_VERSION;
import static com.hazelcast.test.HazelcastTestSupport.assumeThatNoJDK6;
import static com.hazelcast.test.HazelcastTestSupport.assumeThatNoJDK7;
import static com.hazelcast.test.ReflectionsHelper.REFLECTIONS;
import static com.hazelcast.test.ReflectionsHelper.filterNonConcreteClasses;
import static com.hazelcast.test.starter.HazelcastStarterUtils.rethrowGuardianException;
import static com.hazelcast.util.EmptyStatement.ignore;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNoException;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests a common compatibility issue: when a (Identified)DataSerializable class first
 * becomes Versioned, it might miss checking input stream for UNKNOWN version (which is
 * the version of an incoming stream from a previous-version member) instead of using
 * in.getVersion.isUnknownOrLessThan(CURRENT).
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.net.ssl.*", "javax.security.*"})
@PrepareForTest(Version.class)
@Category({QuickTest.class, ParallelTest.class})
public class VersionedInCurrentVersionTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Set<Class<? extends Versioned>> versionedInCurrentVersion;

    @Before
    public void setup() throws Exception {
        assumeThatNoJDK6();
        assumeThatNoJDK7();

        Set<Class<? extends Versioned>> versionedClasses = REFLECTIONS.getSubTypesOf(Versioned.class);
        Set<Class<? extends DataSerializable>> dsClasses = REFLECTIONS.getSubTypesOf(DataSerializable.class);
        Set<Class<? extends Versioned>> versionedSincePreviousVersion = versionedClassesInPreviousVersion();

        filterNonConcreteClasses(versionedClasses);
        versionedClasses.removeAll(versionedSincePreviousVersion);
        versionedClasses.retainAll(dsClasses);

        versionedInCurrentVersion = versionedClasses;
    }

    @Test
    public void testNewVersionedClass_doesNotInvokeLessThan_whenReadingData() {
        List<Class<? extends Versioned>> failures = new ArrayList<Class<? extends Versioned>>();
        for (Class<? extends Versioned> versionedClass : versionedInCurrentVersion) {
            Versioned instance = createInstance(versionedClass);
            if (instance == null) {
                // may occur when there is no default constructor
                continue;
            }
            DataSerializable dataSerializable = (DataSerializable) instance;
            Version spy = org.powermock.api.mockito.PowerMockito.spy(CURRENT_CLUSTER_VERSION);
            ObjectDataInput mockInput = spy(ObjectDataInput.class);
            when(mockInput.getVersion()).thenReturn(spy);
            try {
                dataSerializable.readData(mockInput);
            } catch (Throwable t) {
                ignore(t);
            } finally {
                try {
                    Mockito.verify(spy).isLessThan(CURRENT_CLUSTER_VERSION);
                    failures.add(versionedClass);
                } catch (Throwable t) {
                    // expected when Version.isLessThan() was not invoked
                }
            }
        }
        if (!failures.isEmpty()) {
            StringBuilder failMessageBuilder = new StringBuilder();
            for (Class<? extends Versioned> failedClass : failures) {
                failMessageBuilder.append(StringUtil.LINE_SEPARATOR)
                        .append(failedClass.getName())
                        .append(" invoked in.getVersion().isLessThan(CURRENT_CLUSTER_VERSION) while reading data");
            }
            fail(failMessageBuilder.toString());
        }
    }

    private <T extends Versioned> T createInstance(Class<T> klass) {
        try {
            return klass.newInstance();
        } catch (Exception e) {
            return null;
        }
    }

    private Set<Class<? extends Versioned>> versionedClassesInPreviousVersion() throws Exception {
        File[] previousVersionArtifacts = getPreviousVersionArtifacts();

        URL[] artifactURLs = new URL[previousVersionArtifacts.length];
        for (int i = 0; i < previousVersionArtifacts.length; i++) {
            artifactURLs[i] = previousVersionArtifacts[i].toURI().toURL();
        }
        Reflections previousVersionReflections = new Reflections(new ConfigurationBuilder()
                .setUrls(artifactURLs)
                .addScanners(new SubTypesScanner())
        );

        return previousVersionReflections.getSubTypesOf(Versioned.class);
    }

    private File[] getPreviousVersionArtifacts() throws Exception {
        File previousVersionFolder = temporaryFolder.newFolder();
        try {
            return HazelcastVersionLocator.locateVersion(
                    PREVIOUS_CLUSTER_VERSION.toString(),
                    previousVersionFolder,
                    getBuildInfo().isEnterprise());
        } catch (GuardianException e) {
            assumeNoException("The requested version could not be downloaded, most probably it has not been released yet", e);
            throw rethrowGuardianException(e);
        }
    }
}
