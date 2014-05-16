package com.hazelcast.config;

import java.util.Properties;

import static com.hazelcast.util.ValidationUtil.hasText;

/**
 * Provides configuration for custom cluster Joining strategies.
 *
 * @author dturner@kixeye.com
 */
public class CustomConfig {

    private boolean enabled = false;

    /**
     * fully qualified class name of a JoinerFactory implementation
     */
    private String joinerFactoryClassName;

    /**
     * thread-safe collection of configuration properties
     */
    private Properties properties = new Properties();

    /**
     * Setter for enabled
     * @param enabled
     * @return
     */
    public CustomConfig setEnabled(boolean enabled){
        this.enabled = enabled;
        return this;
    }

    /**
     * Getter for enabled
     * @return
     */
    public boolean isEnabled(){
        return enabled;
    }

    /**
     * Setter for joinerFactoryClassName
     * @param joinerFactoryClassName
     * @return
     */
    public CustomConfig setJoinerFactoryClassName(String joinerFactoryClassName){
        this.joinerFactoryClassName = hasText(joinerFactoryClassName, "joinerFactoryClassName");
        return this;
    }

    /**
     * Getter for joinerFactoryClassName
     * @return
     */
    public String getJoinerFactoryClassName(){
        return this.joinerFactoryClassName;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    /**
     * Returns a copy of the Config's properties.
     * @return
     */
    public Properties getProperties() {
        return properties;
    }
}
