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

package com.hazelcast.instance;

/**
 * This class is generated in a build-time from a template stored at
 * src/main/template/com/hazelcast/instance/GeneratedBuildProperties.
 *
 * Do not edit by hand as the changes will be overwritten in the next build.
 *
 * We used to have the version info as property file, but this caused issues
 * in on environments with a complicated classloading model. Having the info
 * as a Java class provide a better control when you have multiple version of
 * Hazelcast deployed.
 *
 * WARNING: DO NOT CHANGE FIELD NAMES IN THE TEMPLATE.
 * The fields are read via reflection at {@link com.hazelcast.instance.BuildInfoProvider}
 *
 */
public final class GeneratedBuildProperties {
    public static final String VERSION = "${project.version}";
    public static final String BUILD = "${timestamp}";
    public static final String REVISION = "${git.commit.id.abbrev}";
    public static final String COMMIT_ID = "${git.commit.id}";
    public static final String DISTRIBUTION = "${hazelcast.distribution}";
    public static final String SERIALIZATION_VERSION = "${hazelcast.serialization.version}";

    private GeneratedBuildProperties() {
    }
}
