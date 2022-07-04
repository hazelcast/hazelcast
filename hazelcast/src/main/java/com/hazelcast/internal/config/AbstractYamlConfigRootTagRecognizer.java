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

package com.hazelcast.internal.config;

import com.hazelcast.config.ConfigRecognizer;
import com.hazelcast.config.ConfigStream;
import com.hazelcast.internal.yaml.YamlException;
import com.hazelcast.internal.yaml.YamlLoader;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.snakeyaml.engine.v2.api.ConstructNode;
import org.snakeyaml.engine.v2.api.LoadSettingsBuilder;

import java.util.Optional;

/**
 * Abstract {@link ConfigRecognizer} implementation that recognizes
 * Hazelcast YAML configurations. The recognition is done by looking into
 * the provided configuration to check if the root node is the expected
 * one.
 * <p/>
 * This implementation loads the entire YAML document and builds the
 * document's internal Hazelcast YAML representation graph. This can be
 * prevented by creating and using custom {@link ConstructNode}
 * implementations for the tag types.
 * See {@link LoadSettingsBuilder#setRootConstructNode(Optional)}.
 * <p/>
 * If the provided configuration is not a valid YAML document, no exception
 * is thrown. Instead, the configuration is simply not recognized by this
 * implementation.
 * </p>
 * Note that this {@link ConfigRecognizer} doesn't validate the
 * configuration and doesn't look further into the provided configuration.
 */
public abstract class AbstractYamlConfigRootTagRecognizer implements ConfigRecognizer {
    private final String expectedRootNode;
    private final ILogger logger = Logger.getLogger(AbstractYamlConfigRootTagRecognizer.class);

    public AbstractYamlConfigRootTagRecognizer(String expectedRootNode) {
        this.expectedRootNode = expectedRootNode;
    }

    @Override
    public boolean isRecognized(ConfigStream configStream) {
        try {
            YamlLoader.load(configStream, expectedRootNode);
            return true;
        } catch (YamlException ex) {
            handleParseException(ex);

            return false;
        } catch (Exception ex) {
            handleUnexpectedException(ex);
            throw ex;
        }
    }

    private void handleParseException(YamlException ex) {
        if (logger.isFineEnabled()) {
            logger.fine("An exception is encountered while processing the provided YAML configuration", ex);
        }
    }

    private void handleUnexpectedException(Exception ex) {
        if (logger.isFineEnabled()) {
            logger.fine("An unexpected exception is encountered while processing the provided YAML configuration", ex);
        }
    }
}
