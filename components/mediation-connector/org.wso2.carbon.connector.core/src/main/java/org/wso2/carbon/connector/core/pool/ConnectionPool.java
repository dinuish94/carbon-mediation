package org.wso2.carbon.connector.core.pool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.wso2.carbon.connector.core.ConnectException;

import static java.lang.String.format;

/**
 * Represents a connection pool
 */
public class ConnectionPool extends GenericObjectPool {

    private static final Log log = LogFactory.getLog(ConnectionPool.class);

    public ConnectionPool(ConnectionFactory factory, Configuration configuration) {

        super(factory);

        if (configuration.getMaxActiveConnections() != null) {
            this.setMaxActive(configuration.getMaxActiveConnections());
        }
        if (configuration.getMaxIdleConnections() != null) {
            this.setMaxIdle(configuration.getMaxIdleConnections());
        }
        if (configuration.getMinIdleConnections() != null) {
            this.setMinIdle(configuration.getMinIdleConnections());
        }
        if (configuration.getMaxWaitTime() != null) {
            this.setMaxWait(configuration.getMaxWaitTime());
        }
        if (configuration.getMinEvictionTime() != null) {
            this.setMinEvictableIdleTimeMillis(configuration.getMinEvictionTime());
        }
        if (configuration.getEvictionCheckInterval() != null) {
            this.setTimeBetweenEvictionRunsMillis(configuration.getEvictionCheckInterval());
        }
        if (configuration.getExhaustedAction() != null) {
            this.setWhenExhaustedAction(getExhaustedAction(configuration.getExhaustedAction()));
        }
        if (configuration.getTestOnBorrow() != null) {
            this.setTestOnBorrow(configuration.getTestOnBorrow());
        }
        if (configuration.getTestOnReturn() != null) {
            this.setTestOnBorrow(configuration.getTestOnReturn());
        }
        if (configuration.getTestWhileIdle() != null) {
            this.setTestWhileIdle(configuration.getTestWhileIdle());
        }
        if (configuration.getNumTestsPerEvictionRun() != null) {
            this.setNumTestsPerEvictionRun(configuration.getNumTestsPerEvictionRun());
        }
        if (configuration.getSoftMinEvictableIdleTimeMillis() != null) {
            this.setSoftMinEvictableIdleTimeMillis(configuration.getSoftMinEvictableIdleTimeMillis());
        }
    }

    /**
     * Parse exhausted action from string
     *
     * @param exhaustedAction exhausted action in string
     * @return respective byte that represents the action
     */
    private byte getExhaustedAction(String exhaustedAction) {

        byte action;
        switch (exhaustedAction) {
            case "WHEN_EXHAUSTED_FAIL":
                action = WHEN_EXHAUSTED_FAIL;
                break;
            case "WHEN_EXHAUSTED_BLOCK":
                action = WHEN_EXHAUSTED_BLOCK;
                break;
            case "WHEN_EXHAUSTED_GROW":
                action = WHEN_EXHAUSTED_GROW;
                break;
            default:
                action = DEFAULT_WHEN_EXHAUSTED_ACTION;
                log.warn(format("Unable to find the configured exhausted action. Setting to default: %s.", action));
                break;
        }
        return action;
    }

    @Override
    public synchronized Object borrowObject() throws ConnectException {

        try {
            log.debug("Borrowing object from the connection pool...");
            return super.borrowObject();
        } catch (Exception e) {
            throw new ConnectException(e, "Error occurred while borrowing connection from the pool.");
        }
    }

    @Override
    public synchronized void returnObject(Object obj) {

        try {
            log.debug("Returning object to the connection pool...");
            super.returnObject(obj);
        } catch (Exception e) {
            log.error("Error occurred while returning the connection to the pool.", e);
        }
    }

    @Override
    public synchronized void close() throws ConnectException {

        try {
            super.close();
        } catch (Exception e) {
            throw new ConnectException(e, "Error occurred while closing the connections.");
        }
    }
}
