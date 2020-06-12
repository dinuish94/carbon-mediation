package org.wso2.carbon.connector.core.connection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.pool.Configuration;
import org.wso2.carbon.connector.core.pool.ConnectionFactory;
import org.wso2.carbon.connector.core.pool.ConnectionPool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;

/**
 * Handles the connections
 */
public class ConnectionHandler {

    private static final Log log = LogFactory.getLog(ConnectionHandler.class);
    private static ConnectionHandler handler;
    // Stores connections/connection pools against connection code name
    // defined as <connector_name>:<connection_name>
    private Map<String, Object> connectionMap;

    private ConnectionHandler() {

        this.connectionMap = new ConcurrentHashMap<>(new HashMap<>());
    }

    /**
     * Gets the Connection Handler instance
     *
     * @return ConnectionHandler instance
     */
    public static synchronized ConnectionHandler getConnectionHandler() {

        if (handler == null) {
            handler = new ConnectionHandler();
        }
        return handler;
    }

    /**
     * Creates a new connection pool and stores the connection
     *
     * @param connector      Name of the connector
     * @param connectionName Name of the connection
     * @param factory        Connection Factory that defines how to create connections
     * @param configuration  Configurations for the connection pool
     */
    public void createConnection(String connector, String connectionName, ConnectionFactory factory,
                                 Configuration configuration) {

        ConnectionPool pool = new ConnectionPool(factory, configuration);
        connectionMap.putIfAbsent(getCode(connector, connectionName), pool);
    }

    /**
     * Stores a new single connection
     *
     * @param connector      Name of the connector
     * @param connectionName Name of the connection
     * @param connection     Connection to be stored
     */
    public void createConnection(String connector, String connectionName, Connection connection) {

        connectionMap.putIfAbsent(getCode(connector, connectionName), connection);
    }

    /**
     * Retrieve connection by connector name and connection name
     *
     * @param connector      Name of the connector
     * @param connectionName Name of the connection
     * @return the connection
     * @throws ConnectException if failed to get connection
     */
    public Connection getConnection(String connector, String connectionName) throws ConnectException {

        Connection connection = null;
        String connectorCode = getCode(connector, connectionName);
        if (connectionMap.get(connectorCode) != null) {
            Object connectionObj = connectionMap.get(connectorCode);
            if (connectionObj instanceof ConnectionPool) {
                connection = (Connection) ((ConnectionPool) connectionObj).borrowObject();
            } else if (connectionObj instanceof Connection) {
                connection = (Connection) connectionObj;
            }
        } else {
            throw new ConnectException(format("Error occurred during retrieving connection. " +
                    "Connection %s for %s connector does not exist.", connectionName, connector));
        }
        return connection;
    }

    /**
     * Return borrowed connection
     *
     * @param connector      Name of the connector
     * @param connectionName Name of the connection
     */
    public void returnConnection(String connector, String connectionName) {

        String connectorCode = getCode(connector, connectionName);
        if (connectionMap.get(connectorCode) != null) {
            Object connectionObj = connectionMap.get(connectorCode);
            if (connectionObj instanceof ConnectionPool) {
                ((ConnectionPool) connectionObj).returnObject(connectionObj);
            }
        }
    }

    /**
     * Shutdown the connection pools
     */
    public void shutdownConnections() {

        for (Map.Entry<String, Object> connection : connectionMap.entrySet()) {
            Object connectionObj = connection.getValue();
            if (connectionObj instanceof ConnectionPool) {
                try {
                    ((ConnectionPool) connectionObj).close();
                } catch (ConnectException e) {
                    log.error("Failed to close connection pool. ", e);
                }
            }
        }
    }

    /**
     * Check if a connection exists for the connector
     *
     * @param connector      Name of the connector
     * @param connectionName Name of the connection
     * @return
     */
    public boolean checkIfConnectionExists(String connector, String connectionName) {

        return connectionMap.containsKey(getCode(connector, connectionName));
    }

    /**
     * Retrieves the connection code defined as <connector_name>:<connection_name>
     *
     * @param connector      Name of the connector
     * @param connectionName Name of the connection
     * @return the connector code
     */
    private String getCode(String connector, String connectionName) {

        return format("%s:%s", connector, connectionName);
    }

}
