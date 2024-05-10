/*
 * Copyright 2024 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.s3;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MavenVersionUtils {
    /** @return the version of the JAR on the classpath for the given {@code groupId} & {@code artifactId} */
    public static String getMavenVersion(String groupId, String artifactId) throws IOException {
        try (InputStream inputStream = MavenVersionUtils.class
                .getResourceAsStream("/META-INF/maven/" + groupId + "/" + artifactId + "/pom.properties")) {
            Properties properties = new Properties();
            properties.load(inputStream);
            return properties.getProperty("version");
        }
    }
}
