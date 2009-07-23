package com.hazelcast.config;

/**
 * Interface for all config builders.
 */

public interface ConfigBuilder {

	/**
	 * Parses the configuration and stores the values in passed Config object.
	 * 
	 * @param config the Config object to be filled by ConfigBuilder
	 * 
	 * @throws Exception the exception
	 */
	public abstract void parse(final Config config) throws Exception;

}
