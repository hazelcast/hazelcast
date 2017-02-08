/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.net.URL;
import java.util.Collection;
import java.util.Iterator;

/**
 * Initialize once org.reflections library to avoid duplicate work scanning the classpath from individual tests.
 */
public class ReflectionsHelper {

    public static final Reflections REFLECTIONS;

    static {
        // obtain all classpath URLs with com.hazelcast package classes
        Collection<URL> comHazelcastPackageURLs = ClasspathHelper.forPackage("com.hazelcast");
        // exclude hazelcast test artifacts from package URLs
        for (Iterator<URL> iterator = comHazelcastPackageURLs.iterator(); iterator.hasNext();) {
            URL url = iterator.next();
            // detect hazelcast-VERSION-tests.jar & $SOMEPATH/hazelcast/target/test-classes/ and exclude it from classpath
            if (url.toString().contains("-tests.jar") || url.toString().contains("target/test-classes")) {
                iterator.remove();
            }
        }
        REFLECTIONS = new Reflections(new ConfigurationBuilder().addUrls(comHazelcastPackageURLs)
                .addScanners(new SubTypesScanner()).addScanners(new TypeAnnotationsScanner()));
    }
}
