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

package com.hazelcast.sql.impl;

import org.slf4j.helpers.NOP_FallbackServiceProvider;
import org.slf4j.spi.SLF4JServiceProvider;

/**
 * Apache Calcite has a dependency on the {@code slf4j-api} artifact. Due to JAR
 * hell in the runtime, we shade and relocate the {@code slf4j-api} library into
 * the {@code com.hazelcast} package during build.
 *
 * <p>
 * SLF4J requires at least one {@link SLF4JServiceProvider} to be present.
 * However, the required name of the service files changes due to shading, so service provider inside e.g. Log4J2
 * won't work - we need to provide custom file with some implementation, like this no-op implementation
 * in order to suppress warnings.
 *
 * <p>
 * The service provider delegates to the no-op logger service provider, suppressing all messages
 * from the relocated Apache Calcite.
 */
public class NoopSlf4jServiceProvider
        extends NOP_FallbackServiceProvider
        implements SLF4JServiceProvider {
}
