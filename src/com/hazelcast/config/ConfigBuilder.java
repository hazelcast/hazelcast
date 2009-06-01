package com.hazelcast.config;

public interface ConfigBuilder {

	public abstract void parse(final Config config) throws Exception;

}