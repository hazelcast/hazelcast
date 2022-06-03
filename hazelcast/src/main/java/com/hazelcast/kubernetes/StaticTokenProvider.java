package com.hazelcast.kubernetes;

public class StaticTokenProvider implements KubernetesTokenProvider {
    private final String token;

    public StaticTokenProvider(String token) {
        this.token = token;
    }

    @Override
    public String getToken() {
        return token;
    }
}
