/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management;

import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.ClientSelector;
import com.hazelcast.client.impl.ClientSelectors;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.dto.ClientBwListDTO;
import com.hazelcast.internal.management.dto.ClientBwListEntryDTO;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;

import java.util.List;
import java.util.regex.Pattern;

import static com.hazelcast.internal.util.JsonUtil.getObject;

/**
 * Handles client B/W list configuration received from Management Center and applies changes.
 * <p>
 * This class is thread-safe.
 * <p>
 * Note: Once MC migrates to client comms, this class may be converted into a {@link AbstractLocalOperation}.
 * The {@link #handleLostConnection} and {@link #handleConfig} methods won't be required when the migration happens.
 *
 * @see ManagementCenterService ManagementCenterService for more details on usage.
 */
public class ClientBwListConfigHandler {

    private static final ILogger LOGGER = Logger.getLogger(ClientBwListConfigHandler.class);

    private final ClientEngine clientEngine;

    public ClientBwListConfigHandler(ClientEngine clientEngine) {
        this.clientEngine = clientEngine;
    }

    /**
     * Handles Management Center connection lost event: removes any client B/W list filtering.
     */
    public void handleLostConnection() {
        try {
            clientEngine.applySelector(ClientSelectors.any());
        } catch (Exception e) {
            LOGGER.warning("Could not clean up client B/W list filtering.", e);
        }
    }

    /**
     * Parses client B/W list filtering configuration in JSON format and applies the configuration.
     * Never throws.
     *
     * @param configJson configuration JSON object
     */
    public void handleConfig(JsonObject configJson) {
        try {
            JsonObject bwListConfigJson = getObject(configJson, "clientBwList");
            ClientBwListDTO configDTO = new ClientBwListDTO();
            configDTO.fromJson(bwListConfigJson);
            applyConfig(configDTO);
        } catch (Exception e) {
            LOGGER.warning("Could not apply client B/W list filtering.", e);
        }
    }

    /**
     * Applies the given config.
     *
     * @param configDTO configuration object
     */
    public void applyConfig(ClientBwListDTO configDTO) {
        ClientSelector selector;
        switch (configDTO.mode) {
            case DISABLED:
                selector = ClientSelectors.any();
                break;
            case WHITELIST:
                selector = createSelector(configDTO.entries);
                break;
            case BLACKLIST:
                selector = ClientSelectors.inverse(createSelector(configDTO.entries));
                break;
            default:
                throw new IllegalArgumentException("Unknown client B/W list mode: " + configDTO.mode);
        }

        clientEngine.applySelector(selector);
    }

    private static ClientSelector createSelector(List<ClientBwListEntryDTO> entries) {
        ClientSelector selector = ClientSelectors.none();
        for (ClientBwListEntryDTO entryDTO : entries) {
            ClientSelector entrySelector = createSelector(entryDTO);
            selector = ClientSelectors.or(selector, entrySelector);
        }
        return selector;
    }

    private static ClientSelector createSelector(ClientBwListEntryDTO entry) {
        switch (entry.type) {
            case IP_ADDRESS:
                return ClientSelectors.ipSelector(entry.value);
            case INSTANCE_NAME:
                return ClientSelectors.nameSelector(sanitizeValueWithWildcards(entry.value));
            case LABEL:
                return ClientSelectors.labelSelector(sanitizeValueWithWildcards(entry.value));
            default:
                throw new IllegalArgumentException("Unknown client B/W list entry type: " + entry.type);
        }
    }

    /**
     * Sanitizes values in order to replace any regexp-specific char sequences, except for wildcards ('*').
     * First, quotes the input string to escape any regexp-specific char sequences. Second, replaces escaped wildcards
     * with the appropriate char sequence. Example: "green*client" => "\Qgreen\E.*\Qclient\E".
     */
    private static String sanitizeValueWithWildcards(String value) {
        String quoted = Pattern.quote(value);
        return quoted.replaceAll("\\*", "\\\\E.*\\\\Q");
    }

}
