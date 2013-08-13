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

    private final static String HOST_PROPERTY = "host";
    private final static String PORT_PROPERTY = "port";

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

    public int getInt(String key, int def) {
        return config.getInt(key, def);
    }

    public String getHost(String def) {
        return getString(HOST_PROPERTY, def);
    }

    public int getPort(int def) {
        return getInt(PORT_PROPERTY, def);
    }

    @Override
    public String toString() {
        return config.getProperties().toString();
    }
}
