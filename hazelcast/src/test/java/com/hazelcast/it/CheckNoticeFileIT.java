/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.it;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
public class CheckNoticeFileIT {

    @Test
    @Category(QuickTest.class)
    public void verifyNoticeFile() throws IOException, InterruptedException {
        assertResourcePresence(true, "META-INF/NOTICE");
        assertResourcePresence(false, "META-INF/NOTICE.txt");
    }

    private void assertResourcePresence(boolean isResourceExpected, String resourcePath) throws IOException {
        ClassLoader cl = getClassFromPackage().getClassLoader();
        Enumeration<URL> resources = cl.getResources(resourcePath);
        while (resources.hasMoreElements()) {
            String urlString = resources.nextElement().toString();
            if (urlString.contains("target")) {
                if (isResourceExpected) {
                    return;
                } else {
                    Assert.fail("Resource " + resourcePath + " was not expected, but found in " + urlString);
                }
            }
        }
        if (isResourceExpected) {
            Assert.fail("Resource " + resourcePath + " was expected, but was not found.");
        }
    }

    protected Class<?> getClassFromPackage() {
        return HazelcastInstance.class;
    }
}
