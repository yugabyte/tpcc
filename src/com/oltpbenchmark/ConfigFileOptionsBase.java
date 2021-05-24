package com.oltpbenchmark;

import java.util.Optional;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;

import com.oltpbenchmark.util.InvalidUserConfiguration;

public abstract class ConfigFileOptionsBase {
    protected final XMLConfiguration xmlConfig;

    ConfigFileOptionsBase(String filePath) throws ConfigurationException {
        xmlConfig = new XMLConfiguration(filePath);
        xmlConfig.setExpressionEngine(new XPathExpressionEngine());
    }
    
    protected Optional<String> getStringOpt(String key) {
        if (xmlConfig.containsKey(key) && xmlConfig.getString(key).length() > 0) {
            return Optional.of(xmlConfig.getString(key));
        }
        return Optional.empty();
    }

    protected String getRequiredStringOpt(String key) {
        return getStringOpt(key).orElseThrow(() -> new InvalidUserConfiguration("Invalid Configuration: Expected option " + key + " not found"));
    }
    
    protected Optional<Boolean> getBoolOpt(String key) {
        if (xmlConfig.containsKey(key)) {
            return Optional.of(xmlConfig.getBoolean(key));
        }
        return Optional.empty();
    }
    
    protected Optional<Integer> getIntOpt(String key) {
        if (xmlConfig.containsKey(key)) {
            return Optional.of(xmlConfig.getInt(key));
        }
        return Optional.empty();
    }
    
    protected boolean isFlagSet(String key) {
        return getBoolOpt(key).orElse(false);
    }

}
