/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast;

import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;

import java.io.InputStream;
import java.util.Properties;

import static com.hazelcast.internal.tpcengine.util.CloseUtil.closeQuietly;

public final class GitInfo {

    static final String GIT_COMMIT_ID_AABREV = "git.commit.id.abbrev";
    static final String GIT_BUILD_TIME = "git.build.time";

    private static final TpcLogger LOGGER = TpcLoggerLocator.getLogger(GitInfo.class);

    private static final Properties PROPERTIES = loadGitProperties();

    public static String getCommitIdAbbrev() {
        if (PROPERTIES == null) {
            return null;
        }
        return PROPERTIES.getProperty(GIT_COMMIT_ID_AABREV);
    }

    public static String getBuildTime() {
        if (PROPERTIES == null) {
            return null;
        }
        return PROPERTIES.getProperty(GIT_BUILD_TIME);
    }

    static Properties loadGitProperties() {
        String fileName = "tpc-tools-git.properties";
        Properties properties = new Properties();
        InputStream inputStream = GitInfo.class.getClassLoader().getResourceAsStream(fileName);
        try {
            properties.load(inputStream);
            return properties;
        } catch (NullPointerException e) {
            LOGGER.fine("Error while loading Git properties from " + fileName, e);
        } catch (Exception e) {
            LOGGER.warning("Error while loading Git properties from " + fileName, e);
        } finally {
            closeQuietly(inputStream);
        }
        return null;
    }

}
