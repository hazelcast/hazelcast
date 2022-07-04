/*
 * Copyright 2021 Hazelcast Inc.
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

package org.slf4j.impl;

import org.slf4j.ILoggerFactory;
import org.slf4j.helpers.NOPLoggerFactory;
import org.slf4j.spi.LoggerFactoryBinder;

/**
 * Apache Calcite has a dependency on the {@code slf4j-api} artifact. In order to JAR hell in the runtime, we shade and
 * relocate the {@code slf4j-api} library into the {@code com.hazelcast} package during build.
 * <p>
 * SLF4J requires exactly one {@code StaticLoggerBinder} class to be in the JVM classpath. Otherwise a warning is printed
 * into the {@code System.err} unconditionally. See {@code LoggerFactory.bind} for more details.
 * <p>
 * This class helps us get rid of the warning. During relocation both this class and {@code LoggerFactory} are moved to the
 * the {@code com.hazelcast} package. As a result the relocated factory finds the relocated binder and no warning is produced.
 * <p>
 * At the same time, the normal SLF4J dependency still looks for the binder at the usual location, and therefore other runtime
 * classes that use SLF4J are not affected. That is, the current binder affects only the relocated Apache Calcite classes.
 * <p>
 * The binder delegates to the no-op logger factory, suppressing all messages from the relocated Apache Calcite.
 */
public class StaticLoggerBinder implements LoggerFactoryBinder {

    private static final StaticLoggerBinder SINGLETON = new StaticLoggerBinder();

    public static StaticLoggerBinder getSingleton() {
        return SINGLETON;
    }

    @Override
    public ILoggerFactory getLoggerFactory() {
        return new NOPLoggerFactory();
    }

    @Override
    public String getLoggerFactoryClassStr() {
        return StaticLoggerBinder.class.getName();
    }
}
