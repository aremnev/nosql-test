package net.thumbtack.research.nosql;

import com.netflix.config.ConcurrentMapConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: vkornev
 * Date: 12.08.13
 * Time: 18:09
 *
 * Class for work with configs
 */
public class Configurator {
    private static final Logger log = LoggerFactory.getLogger(Configurator.class);
    private final ConcurrentMapConfiguration config;

    private final static String DB_NAME_PROPERTY = "db.name";
    private final static String DB_HOST_PROPERTY = "db.hosts";
    private final static String DB_PORT_PROPERTY = "db.port";
    private final static String DB_RETRIES_PROPERTY = "db.retries";
    private final static String DB_SLOW = "db.slow";
    private final static String SC_NAME_PROPERTY = "sc.name";
    private final static String SC_THREADS_PROPERTY = "sc.threads";
    private final static String SC_WRITES_PROPERTY = "sc.writes";
    private final static String SC_STRING_SIZE_PROPERTY = "sc.stringSize";
    private final static String REPORT_FLUSH_INTERVAL_PROPERTY = "report.flushInterval";
    private String[] hosts;
    private String slow;
    private int hostsIdx = -1;

    public Configurator(String fileName) {
        try {
            config = new ConcurrentMapConfiguration(new PropertiesConfiguration(fileName));
        } catch (ConfigurationException e) {
            e.printStackTrace();
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public String getString(String key, String def) {
        return config.getString(key, def);
    }

    public int getInt(String key, Integer def) {
        return config.getInteger(key, def);
    }

    public long getLong(String key, Long def) {
        return config.getLong(key, def);
    }

    public String getDbName() {
        return getString(DB_NAME_PROPERTY, null);
    }

    public String[] getDbHosts() {
        hosts = config.getStringArray(DB_HOST_PROPERTY);
        return hosts;
    }

    public String getNextDbHost(String def) {
        if(hosts == null) {
            getDbHosts();
        }
        if (hosts.length == 0) {
            return def;
        }
        hostsIdx++;
        if(hostsIdx >= hosts.length) {
            hostsIdx = 0;
        }
        return hosts[hostsIdx];
    }

    public boolean isSlow(String host) {
        if(slow == null) {
            slow = config.getString(DB_SLOW, "");
        }
        if(host == null || host.equals("")) {
            return false;
        }
        return host.equals(slow);
    }

    public int getDbPort(int def) {
        return getInt(DB_PORT_PROPERTY, def);
    }

    public int getDbRetries(int def) {
        return getInt(DB_RETRIES_PROPERTY, def);
    }

    public String getScName() {
        return getString(SC_NAME_PROPERTY, null);
    }

    public int getScThreads() {
        return getInt(SC_THREADS_PROPERTY, null);
    }

    public long getScWrites() {
        return getLong(SC_WRITES_PROPERTY, null);
    }

    public long getSCStringSize() {
        return getLong(SC_STRING_SIZE_PROPERTY, null);
    }

    public int getReportFlushInterval() {
        return getInt(REPORT_FLUSH_INTERVAL_PROPERTY, null);
    }

    @Override
    public String toString() {
        return config.getProperties().toString();
    }
}
