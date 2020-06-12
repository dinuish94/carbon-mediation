package org.wso2.carbon.connector.core.pool;

import org.apache.commons.pool.PoolableObjectFactory;

/**
 * This class wraps the PoolableObjectFactory
 * Defines how the connection objects are created, validated and destroyed
 */
public interface ConnectionFactory extends PoolableObjectFactory {

}
