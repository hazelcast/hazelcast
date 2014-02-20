package com.hazelcast.config;

import com.hazelcast.nio.MemberAttributes;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * User: grant
 */
public class MemberAttributesConfig {

    private Map<String, MemberAttributeConfig> attributes = new HashMap<String, MemberAttributeConfig>();

    public Map<String, MemberAttributeConfig> getMemberAttributeConfigs() {
        return attributes;
    }

    public void setMemberAttributeConfigs(Map<String, MemberAttributeConfig> memberAttributeConfigs) {
        this.attributes = memberAttributeConfigs;
    }

    public MemberAttributesConfig clear() {
        attributes.clear();
        return this;
    }

    public MemberAttributeConfig getMemberAttributeConfig(String key) {
        return attributes.get(key);
    }


    public MemberAttributesConfig setMemberAttributeConfigs(Collection<MemberAttributeConfig> configs) {
        clear();
        for (MemberAttributeConfig config : configs) {
            addAttributeConfig(config);
        }
        return this;
    }

    public MemberAttributesConfig addAttributeConfig(MemberAttributeConfig config) {
        attributes.put(config.getName(), config);
        return this;
    }

    public MemberAttributes getMemberAttributes() {
        MemberAttributes atts = new MemberAttributes();
        for (MemberAttributeConfig config : attributes.values()) {
            atts.addAttribute(config.getName(), config.getValue());
        }
        return atts;
    }


}
