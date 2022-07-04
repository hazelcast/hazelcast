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

package com.hazelcast.instance.impl;

import static com.hazelcast.spi.properties.ClusterProperty.LOG_EMOJI_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.SECURITY_RECOMMENDATIONS;
import static com.hazelcast.spi.properties.ClusterProperty.SOCKET_SERVER_BIND_ANY;

import java.util.Map;
import java.util.logging.Level;

import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.AuditlogConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EncryptionAtRestConfig;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.PersistenceConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.properties.HazelcastProperties;

/**
 * Class resposible for printing security recommendation to the log during the {@link Node} start.
 */
class NodeSecurityBanner {

    protected static final String SECURITY_BANNER_CATEGORY = "com.hazelcast.system.security";
    private final Config config;
    private final HazelcastProperties properties;
    private final boolean multicastUsed;
    private final ILogger securityLogger;
    private final boolean showEmoji;

    NodeSecurityBanner(Config config, HazelcastProperties properties, boolean multicastUsed, LoggingService loggingService) {
        this.config = config;
        this.properties = properties;
        this.multicastUsed = multicastUsed;
        this.securityLogger = loggingService.getLogger(SECURITY_BANNER_CATEGORY);
        this.showEmoji = properties.getBoolean(LOG_EMOJI_ENABLED);
    }

    public void printSecurityInfo() {
        boolean showInfoSecurityBanner = properties.getString(SECURITY_RECOMMENDATIONS) != null;
        if ((showInfoSecurityBanner && securityLogger.isInfoEnabled()) || securityLogger.isFineEnabled()) {
            printSecurityFeaturesInfo(config, showInfoSecurityBanner ? Level.INFO : Level.FINE);
        } else {
            securityLogger.info(String.format("Enable DEBUG/FINE log level for log category %s "
                    + " or use -D%s system property to see %ssecurity recommendations and the status of current config.",
                    SECURITY_BANNER_CATEGORY, SECURITY_RECOMMENDATIONS.getName(), getLockEmo()));
        }
    }

    @SuppressWarnings({ "checkstyle:CyclomaticComplexity", "checkstyle:MethodLength" })
    private void printSecurityFeaturesInfo(Config config, Level logLevel) {
        StringBuilder sb = new StringBuilder("\n").append(getLockEmo()).append("Security recommendations and their status:");
        addSecurityFeatureCheck(sb, "Use a custom cluster name", !Config.DEFAULT_CLUSTER_NAME.equals(config.getClusterName()));
        addSecurityFeatureCheck(sb, "Disable member multicast discovery/join method", !multicastUsed);

        AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
        addSecurityFeatureCheck(sb, "Use advanced networking, separate client and member sockets",
                advancedNetworkConfig.isEnabled());
        boolean bindAny = properties.getBoolean(SOCKET_SERVER_BIND_ANY);
        addSecurityFeatureCheck(sb,
                "Bind Server sockets to a single network interface (disable " + SOCKET_SERVER_BIND_ANY.getName() + ")",
                !bindAny);
        StringBuilder tlsSb = new StringBuilder();
        boolean tlsUsed = true;
        if (advancedNetworkConfig.isEnabled()) {
            for (Map.Entry<EndpointQualifier, EndpointConfig> e : advancedNetworkConfig.getEndpointConfigs().entrySet()) {
                tlsUsed = addAdvNetworkTlsInfo(tlsSb, e.getKey(), e.getValue().getSSLConfig()) && tlsUsed;
            }
        } else {
            SSLConfig sslConfig = config.getNetworkConfig().getSSLConfig();
            tlsUsed = addSecurityFeatureCheck(tlsSb, "Use TLS communication protection (Enterprise)",
                    sslConfig != null && sslConfig.isEnabled());
        }
        boolean jetEnabled = config.getJetConfig().isEnabled();
        if (jetEnabled) {
            boolean trustedEnv = tlsUsed || !bindAny;
            addSecurityFeatureCheck(sb, "Use Jet in trusted environments only (single network interface and/or TLS enabled)",
                    trustedEnv);
            if (config.getJetConfig().isResourceUploadEnabled()) {
                addSecurityFeatureInfo(sb, "Jet resource upload is enabled. Any uploaded code can be executed within "
                        + "Hazelcast. Use this in trusted environments only.");
            }
        }
        if (config.getUserCodeDeploymentConfig().isEnabled()) {
            addSecurityFeatureInfo(sb, "User code deployment is enabled. Any uploaded code can be executed within "
                    + "Hazelcast. Use this in trusted environments only.");
        }
        addSecurityFeatureCheck(sb, "Disable scripting in the Management Center",
                !config.getManagementCenterConfig().isScriptingEnabled());
        addSecurityFeatureCheck(sb, "Disable console in the Management Center",
                !config.getManagementCenterConfig().isConsoleEnabled());
        SecurityConfig securityConfig = config.getSecurityConfig();
        boolean securityEnabled = securityConfig != null && securityConfig.isEnabled();

        addSecurityFeatureCheck(sb, "Enable Security (Enterprise)", securityEnabled);
        if (securityEnabled) {
            checkAuthnConfigured(sb, securityConfig, "member-authentication", securityConfig.getMemberRealm());
            checkAuthnConfigured(sb, securityConfig, "client-authentication", securityConfig.getClientRealm());
        }
        // TLS here
        sb.append(tlsSb.toString());
        PersistenceConfig persistenceConfig = config.getPersistenceConfig();
        if (persistenceConfig != null && persistenceConfig.isEnabled()) {
            EncryptionAtRestConfig encryptionAtRestConfig = persistenceConfig.getEncryptionAtRestConfig();
            addSecurityFeatureCheck(sb, "Enable encryption-at-rest in the Persistence config (Enterprise)",
                    encryptionAtRestConfig != null && encryptionAtRestConfig.isEnabled());
        }
        AuditlogConfig auditlogConfig = config.getAuditlogConfig();
        addSecurityFeatureCheck(sb, "Enable auditlog (Enterprise)", auditlogConfig != null && auditlogConfig.isEnabled());

        sb.append("\nCheck the hazelcast-security-hardened.xml/yaml example config file to find why and how to configure"
                + " these security related settings.\n");
        securityLogger.log(logLevel, sb.toString());
    }

    private void checkAuthnConfigured(StringBuilder sb, SecurityConfig securityConfig, String authName, String realmName) {
        RealmConfig rc = securityConfig.getRealmConfig(realmName);
        addSecurityFeatureCheck(sb, "Configure " + authName + " explicitly (Enterprise)",
                rc != null && rc.isAuthenticationConfigured());
    }

    private boolean addAdvNetworkTlsInfo(StringBuilder sb, EndpointQualifier endpoint, SSLConfig sslConfig) {
        return addSecurityFeatureCheck(sb, "Use TLS in the " + endpoint.toMetricsPrefixString() + " endpoint (Enterprise)",
                sslConfig != null && sslConfig.isEnabled());
    }

    private boolean addSecurityFeatureCheck(StringBuilder sb, String feature, boolean enabled) {
        sb.append("\n  ").append(enabled ? getCheckEmo() : getWarningEmo()).append(feature);
        return enabled;
    }

    private void addSecurityFeatureInfo(StringBuilder sb, String feature) {
        sb.append("\n  ").append(getInfoEmo()).append(feature);
    }

    private String getLockEmo() {
        return showEmoji ? "🔒 " : "";
    }

    private String getInfoEmo() {
        return showEmoji ? "ℹ️ " : "(i) ";
    }

    private String getWarningEmo() {
        return showEmoji ? "⚠️ " : "[ ] ";
    }

    private String getCheckEmo() {
        return showEmoji ? "✅ " : "[X] ";
    }

}
