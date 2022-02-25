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

package com.hazelcast.internal.management;

import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.ClientSelector;
import com.hazelcast.client.impl.ClientSelectors;
import com.hazelcast.internal.management.dto.ClientBwListDTO;
import com.hazelcast.internal.management.dto.ClientBwListEntryDTO;

import java.util.List;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * Handles client B/W list configuration received from Management Center and applies changes.
 * <p>
 * This class is thread-safe.
 *
 * @see ManagementCenterService ManagementCenterService for more details on usage.
 */
public class ClientBwListConfigHandler {

    private final ClientEngine clientEngine;

    public ClientBwListConfigHandler(ClientEngine clientEngine) {
        this.clientEngine = clientEngine;
    }

    /**
     * Applies the given config.
     *
     * @param configDTO configuration object
     */
    public void applyConfig(ClientBwListDTO configDTO) {
        requireNonNull(configDTO, "Client filtering config must not be null");
        requireNonNull(configDTO.mode, "Config mode must not be null");
        requireNonNull(configDTO.entries, "Config entries must not be null");

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
        requireNonNull(entry.type, "Entry type must not be null");
        requireNonNull(entry.value, "Entry value must not be null");

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
