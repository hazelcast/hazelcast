package com.hazelcast.spring.security;

import com.hazelcast.config.GroupConfig;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.ICredentialsFactory;

import java.util.Properties;

public class DummyCredentialsFactory implements ICredentialsFactory {

    public void configure(GroupConfig groupConfig, Properties properties) {
    }

    public Credentials newCredentials() {
        return null;
    }

    public void destroy() {
    }
}
