package com.hazelcast.config;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    private String joinerFactoryClass;

    /**
     * thread-safe collection of configuration properties
     */
    private ConcurrentHashMap<String, String> properties = new ConcurrentHashMap<String, String>();

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
     * Setter for joinerFactoryClass
     * @param joinerFactoryClass
     * @return
     */
    public CustomConfig setJoinerFactoryClass(String joinerFactoryClass){
        this.joinerFactoryClass = hasText(joinerFactoryClass, "joinerFactoryClass");
        return this;
    }

    /**
     * Getter for joinerFactoryClass
     * @return
     */
    public String getJoinerFactoryClass(){
        return this.joinerFactoryClass;
    }

    /**
     * Adds or replaces a property within the Config
     * @param key
     * @param value
     * @return
     */
    public CustomConfig setProperty(String key, String value){
        properties.put(hasText(key, "property key"),hasText(value, "property value"));
        return this;
    }

    /**
     * Returns a copy of the Config's properties.
     * @return
     */
    public Map<String, String> getProperties() {
        return new HashMap<String, String>(properties);
    }
}
