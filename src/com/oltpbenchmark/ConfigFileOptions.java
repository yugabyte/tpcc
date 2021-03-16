package com.oltpbenchmark;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;

import java.util.Optional;

public class ConfigFileOptions {
    XMLConfiguration xmlConfig;

    ConfigFileOptions(String filePath) throws ConfigurationException {
        xmlConfig = new XMLConfiguration(filePath);
        xmlConfig.setExpressionEngine(new XPathExpressionEngine());
    }

    public String getDbDriver() {
        return xmlConfig.getString("driver");
    }

    public String getDbName() {
        return xmlConfig.getString("DBName");
    }

    public String getDbUsername() {
        return xmlConfig.getString("username");
    }

    public String getDbPassword() {
        return xmlConfig.getString("password");
    }

    private Optional<String> getStringOpt(String key) {
        if (xmlConfig.containsKey(key) && xmlConfig.getString(key).length() > 0) {
            return Optional.of(xmlConfig.getString(key));
        }
        return Optional.empty();
    }

    public Optional<String> getSslCert() {
        return getStringOpt("sslCert");
    }

    public Optional<String> getSslKey() {
        return getStringOpt("sslKey");
    }

    public Optional<String> getJdbcUrl() {
        return getStringOpt("jdbcURL");
    }

    public Optional<String> getIsolationLevel() {
        return getStringOpt("isolation");
    }

    private Optional<Boolean> getBoolOpt(String key) {
        if (xmlConfig.containsKey(key)) {
            return Optional.of(xmlConfig.getBoolean(key));
        }
        return Optional.empty();
    }

    public Optional<Boolean> getUseKeyingTime() {
        return getBoolOpt("useKeyingTime");
    }

    public Optional<Boolean> getUseThinkTime() {
        return getBoolOpt("useThinkTime");
    }

    public Optional<Boolean> getEnableForeignKeysAfterLoad() {
        return getBoolOpt("enableForeignKeysAfterLoad");
    }

    public Optional<Boolean> getTrackPerSQLStmtLatencies() {
        return getBoolOpt("trackPerSQLStmtLatencies");
    }

    public Optional<Boolean> getUseStoredProcedures() {
        return getBoolOpt("useStoredProcedures");
    }

    private Optional<Integer> getIntOpt(String key) {
        if (xmlConfig.containsKey(key)) {
            return Optional.of(xmlConfig.getInt(key));
        }
        return Optional.empty();
    }

    public Optional<Integer> getBatchSize() {
        return getIntOpt("batchSize");
    }

    public Optional<Integer> getMaxRetriesPerTransaction() {
        return getIntOpt("maxRetriesPerTransaction");
    }

    public Optional<Integer> getMaxLoaderRetries() {
        return getIntOpt("maxLoaderRetries");
    }

    public Optional<Integer> getPort() {
        return getIntOpt("port");
    }

    public Optional<Integer> getHikariConnectionTimeoutMs() {
        return getIntOpt("hikariConnectionTimeoutMs");
    }

    public int getTransactionTypeCount() {
        return xmlConfig.getList("transactiontypes/transaction/name").size();
    }

    public Optional<String> getTransactionTypeName(int idx) {
        return getStringOpt(String.format("transactiontypes/transaction[%d]/name", idx));
    }

    public Optional<String> getTransactionTypeWeight(int idx) {
        return getStringOpt(String.format("transactiontypes/transaction[%d]/weight", idx));
    }

    public Optional<Integer> getRate() {
        return getIntOpt("rate");
    }

    public Optional<Integer> getRuntime() {
        return getIntOpt("runtime");
    }

}
