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

package com.hazelcast.client.impl;

import com.hazelcast.client.Client;
import com.hazelcast.internal.util.AddressUtil;

import java.util.Set;

public final class ClientSelectors {

    private ClientSelectors() {

    }

    public static ClientSelector any() {
        return new ClientSelector() {
            @Override
            public boolean select(Client client) {
                return true;
            }

            @Override
            public String toString() {
                return "ClientSelector{any}";
            }
        };
    }

    public static ClientSelector none() {
        return new ClientSelector() {
            @Override
            public boolean select(Client client) {
                return false;
            }

            @Override
            public String toString() {
                return "ClientSelector{none}";
            }
        };
    }

    /**
     * Regular regex rules are applied. see String.matches
     *
     * @param nameMask of the clients that will be selected
     * @return client selector according to name
     */
    public static ClientSelector nameSelector(final String nameMask) {
        return new ClientSelector() {
            @Override
            public boolean select(Client client) {
                String name = client.getName();
                if (name == null) {
                    return false;
                }
                return name.matches(nameMask);
            }

            @Override
            public String toString() {
                return "ClientSelector{nameMask:" + nameMask + " }";
            }
        };
    }

    /**
     * Works with AddressUtil.mathInterface
     *
     * @param ipMask ip mask for the selector
     * @return client selector according to IP
     */
    public static ClientSelector ipSelector(final String ipMask) {
        return new ClientSelector() {
            @Override
            public boolean select(Client client) {
                return AddressUtil.matchInterface(client.getSocketAddress().getAddress().getHostAddress(), ipMask);
            }

            @Override
            public String toString() {
                return "ClientSelector{ipMask:" + ipMask + " }";
            }
        };
    }

    public static ClientSelector or(final ClientSelector... selectors) {
        return new ClientSelector() {
            @Override
            public boolean select(Client client) {
                for (ClientSelector selector : selectors) {
                    if (selector.select(client)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public String toString() {
                StringBuilder builder = new StringBuilder();
                builder.append("ClientSelector{or:");
                for (ClientSelector selector : selectors) {
                    builder.append(selector).append(", ");
                }
                builder.append("}");
                return builder.toString();
            }
        };
    }

    /**
     * Regular regex rules are applied. see String.matches
     *
     * @param labelMask of the clients that will be selected
     * @return client selector according to label
     */
    public static ClientSelector labelSelector(final String labelMask) {
        return new ClientSelector() {
            @Override
            public boolean select(Client client) {
                Set<String> labels = client.getLabels();
                for (String label : labels) {
                    if (label.matches(labelMask)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public String toString() {
                return "ClientSelector{labelMask:" + labelMask + " }";
            }
        };
    }

    public static ClientSelector inverse(final ClientSelector clientSelector) {
        return new ClientSelector() {
            @Override
            public boolean select(Client client) {
                return !clientSelector.select(client);
            }

            @Override
            public String toString() {
                return "ClientSelector{inverse:" + clientSelector + " }";
            }
        };
    }

}
