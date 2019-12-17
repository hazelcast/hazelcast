/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.felix.utils.manifest.Clause;
import org.apache.felix.utils.manifest.Parser;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Manifest;

import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class CheckDependenciesIT extends HazelcastTestSupport {


    private static final String MANIFEST_PATH = "META-INF/MANIFEST.MF";
    private static final String[] WHITELIST_PREFIXES = new String[]{

            // everything from the Java package is OK - it's part of the Java SE platform
            "java.",

            // with the "javax" package we have to be more specific - do not use just "javax."
            // as it contains e.g. javax.servlet which is not part of the SE platform!
            "javax.crypto",
            "javax.management",
            "javax.net.ssl",
            "javax.script",
            "javax.security.auth",
            "javax.transaction.xa",
            "javax.xml",
            "javax.naming",

            // these 2 XML-related packages are part of the platform since Java SE 6
            "org.xml.sax",
            "org.w3c.dom",
    };

    @Test
    public void testNoMandatoryDependencyDeclared() throws IOException, InterruptedException {
        Manifest manifest = getHazelcastManifest();
        String packages = manifest.getMainAttributes().getValue("Import-Package");


        Clause[] clauses = Parser.parseHeader(packages);
        for (Clause clause : clauses) {
            String name = clause.getName();
            String resolution = clause.getDirective("resolution");
            checkImport(name, resolution);
        }
    }

    private Manifest getHazelcastManifest() throws IOException {
        URL hazelcastAllManifestUrl = findHazelcastManifestURL();
        InputStream inputStream = null;
        try {
            inputStream = hazelcastAllManifestUrl.openStream();
            return new Manifest(inputStream);
        } finally {
            IOUtil.closeResource(inputStream);
        }
    }

    private void checkImport(String name, String resolution) {
        if ("optional".equals(resolution)) {
            return;
        }
        if (isWhitelisted(name)) {
            return;
        }

        fail("Import " + name + " is not declared as optional");
    }

    private boolean isWhitelisted(String name) {
        String[] whitelistPrefixes = getWhitelistPrefixes();
        for (String whitelistPrefix : whitelistPrefixes) {
            if (name.startsWith(whitelistPrefix)) {
                return true;
            }
        }
        return false;
    }

    private URL findHazelcastManifestURL() throws IOException {
        ClassLoader cl = HazelcastInstance.class.getClassLoader();
        Enumeration<URL> resources = cl.getResources(MANIFEST_PATH);
        URL matchedUrl = null;
        while (resources.hasMoreElements()) {
            URL url = resources.nextElement();
            String urlString = url.toString();
            if (isMatching(urlString)) {
                if (matchedUrl == null) {
                    matchedUrl = url;
                } else {
                    throw new AssertionError("Found multiple matching URLs: " + url + " and " + matchedUrl);
                }
            }
        }
        if (matchedUrl == null) {
            throw new AssertionError("Cannot find Hazelcast manifest");
        }
        return matchedUrl;
    }

    protected String[] getWhitelistPrefixes() {
        return WHITELIST_PREFIXES;
    }

    protected boolean isMatching(String urlString) {
        return urlString.contains("hazelcast-4.") && urlString.contains("target");
    }
}
