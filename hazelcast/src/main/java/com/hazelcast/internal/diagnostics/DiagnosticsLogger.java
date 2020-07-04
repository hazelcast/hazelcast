/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.logging.ILogger;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Forwards the diagnostic plugin output to a Hazelcast {@link ILogger}.
 */
final class DiagnosticsLogger implements DiagnosticsLog {
    private final Diagnostics diagnostics;
    private final ILogger logger;
    private final ILogger diagnosticsLogger;
    private final DiagnosticsLogWriterImpl logWriter;
    private final StringWriter stringWriter;
    private boolean staticPluginsRendered;

    DiagnosticsLogger(Diagnostics diagnostics) {
        this.diagnostics = diagnostics;
        this.logger = diagnostics.logger;
        this.diagnosticsLogger = diagnostics.loggingService.getLogger("com.hazelcast.diagnostics");
        this.logWriter = new DiagnosticsLogWriterImpl(diagnostics.includeEpochTime);
        this.stringWriter = new StringWriter();
        logWriter.init(new PrintWriter(stringWriter));
        logger.info("Sending diagnostics to the 'com.hazelcast.diagnostics' logger");
    }

    public void write(DiagnosticsPlugin plugin) {
        try {
            if (!staticPluginsRendered) {
                renderStaticPlugins();
                staticPluginsRendered = true;
            }

            renderPlugin(plugin);
            if (stringWriter.getBuffer().length() > 0) {
                String message = stringWriter.toString();
                diagnosticsLogger.fine(message);
                stringWriter.getBuffer().setLength(0);
            }
        } catch (RuntimeException e) {
            logger.warning("Failed to write to log: ", e);
        }
    }

    private void renderStaticPlugins() {
        for (DiagnosticsPlugin plugin : diagnostics.staticTasks.get()) {
            renderPlugin(plugin);
        }
    }

    private void renderPlugin(DiagnosticsPlugin plugin) {
        plugin.run(logWriter);
    }
}
