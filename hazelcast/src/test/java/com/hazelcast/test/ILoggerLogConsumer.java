/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;

/**
 * A log consumer, that will append logs into Hazelcast's {@link ILogger}.
 * Usage:
 * <pre>{@code
 * container.withLogConsumer(new ILoggerLogConsumer(log).withPrefix("Docker " + dbDockerImage));
 * }</pre>
 */
public class ILoggerLogConsumer extends BaseConsumer<ILoggerLogConsumer> {

    private final ILogger logger;

    private boolean separateOutputStreams;

    private String prefix = "";

    /**
     * @param logger logger to use
     */
    public ILoggerLogConsumer(ILogger logger) {
        this(logger, false);
    }

    /**
     * @param logger logger to use
     * @param separateOutputStreams If set to true, the output streams will be separated,
     *                              meaning log messages will have type of output stream (out, err) printed.
     */
    public ILoggerLogConsumer(ILogger logger, boolean separateOutputStreams) {
        this.logger = logger;
        this.separateOutputStreams = separateOutputStreams;
    }

    /**
     * Optional prefix, that will be added before each log message.
     */
    public ILoggerLogConsumer withPrefix(String prefix) {
        this.prefix = "[" + prefix + "] ";
        return this;
    }

    /**
     * If set, the output streams will be separated, meaning log messages will have type of output stream (out, err) printed.
     */
    public ILoggerLogConsumer withSeparateOutputStreams() {
        this.separateOutputStreams = true;
        return this;
    }

    @Override
    public void accept(OutputFrame outputFrame) {
        final OutputFrame.OutputType outputType = outputFrame.getType();
        final String utf8String = outputFrame.getUtf8StringWithoutLineEnding();
        String realPrefix = prefix.isEmpty() ? "" : (prefix + ": ");
        switch (outputType) {
            case END:
                break;
            case STDOUT:
                if (separateOutputStreams) {
                    logger.info(realPrefix + utf8String);
                } else {
                    logger.info(prefix + outputType + ": " + utf8String);
                }
                break;
            case STDERR:
                if (separateOutputStreams) {
                    logger.severe(realPrefix + utf8String);
                } else {
                    logger.severe(prefix + outputType + ": " + utf8String);
                }
                break;
            default:
                throw new IllegalArgumentException("Unexpected outputType " + outputType);
        }
    }

}
