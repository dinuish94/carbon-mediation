package org.wso2.carbon.connector.core.pool;

/**
 * Holds the connection pool configurations
 */
public class Configuration {

    private Integer maxActiveConnections;
    private Integer maxIdleConnections;
    private Integer minIdleConnections;
    private Long maxWaitTime;
    private Long minEvictionTime;
    private Long evictionCheckInterval;
    private String exhaustedAction;
    private Boolean testOnReturn;
    private Boolean testOnBorrow;
    private Boolean testWhileIdle;
    private Integer numTestsPerEvictionRun;
    private Long softMinEvictableIdleTimeMillis;

    public Integer getMaxActiveConnections() {

        return maxActiveConnections;
    }

    public void setMaxActiveConnections(Integer maxActiveConnections) {

        this.maxActiveConnections = maxActiveConnections;
    }

    public Integer getMaxIdleConnections() {

        return maxIdleConnections;
    }

    public void setMaxIdleConnections(Integer maxIdleConnections) {

        this.maxIdleConnections = maxIdleConnections;
    }

    public Integer getMinIdleConnections() {

        return minIdleConnections;
    }

    public void setMinIdleConnections(Integer minIdleConnections) {

        this.minIdleConnections = minIdleConnections;
    }

    public Long getMaxWaitTime() {

        return maxWaitTime;
    }

    public void setMaxWaitTime(Long maxWaitTime) {

        this.maxWaitTime = maxWaitTime;
    }

    public Long getMinEvictionTime() {

        return minEvictionTime;
    }

    public void setMinEvictionTime(Long minEvictionTime) {

        this.minEvictionTime = minEvictionTime;
    }

    public Long getEvictionCheckInterval() {

        return evictionCheckInterval;
    }

    public void setEvictionCheckInterval(Long evictionCheckInterval) {

        this.evictionCheckInterval = evictionCheckInterval;
    }

    public String getExhaustedAction() {

        return exhaustedAction;
    }

    public void setExhaustedAction(String exhaustedAction) {

        this.exhaustedAction = exhaustedAction;
    }

    public Boolean getTestOnReturn() {

        return testOnReturn;
    }

    public void setTestOnReturn(Boolean testOnReturn) {

        this.testOnReturn = testOnReturn;
    }

    public Boolean getTestOnBorrow() {

        return testOnBorrow;
    }

    public void setTestOnBorrow(Boolean testOnBorrow) {

        this.testOnBorrow = testOnBorrow;
    }

    public Boolean getTestWhileIdle() {

        return testWhileIdle;
    }

    public void setTestWhileIdle(Boolean testWhileIdle) {

        this.testWhileIdle = testWhileIdle;
    }

    public Integer getNumTestsPerEvictionRun() {

        return numTestsPerEvictionRun;
    }

    public void setNumTestsPerEvictionRun(Integer numTestsPerEvictionRun) {

        this.numTestsPerEvictionRun = numTestsPerEvictionRun;
    }

    public Long getSoftMinEvictableIdleTimeMillis() {

        return softMinEvictableIdleTimeMillis;
    }

    public void setSoftMinEvictableIdleTimeMillis(Long softMinEvictableIdleTimeMillis) {

        this.softMinEvictableIdleTimeMillis = softMinEvictableIdleTimeMillis;
    }
}
