package com.hazelcast.instance;

import com.hazelcast.util.HazelcastUtil;

public final class BuildInfoProvider {

    private BuildInfoProvider() {
    }

    public static BuildInfo getBuildInfo() {
        return BuildInfoHolder.INFO;
    }

    private static final class BuildInfoHolder {
        static final BuildInfo INFO = createNew();
    }

    private static BuildInfo createNew() {
        final String version = HazelcastUtil.getVersion();
        final String build = HazelcastUtil.getBuild();
        final int buildNumber = HazelcastUtil.getBuildNumber();
        final boolean enterprise = HazelcastUtil.isEnterprise();
        return new BuildInfo(version, build, buildNumber, enterprise);
    }

}
