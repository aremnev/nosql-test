package net.thumbtack.research.nosql;

import com.netflix.config.ConcurrentMapConfiguration;
import com.netflix.config.DynamicPropertyFactory;
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
    private final ConcurrentMapConfiguration propertyFactory;

    public Configurator(String fileName) {
        try {
            propertyFactory = new ConcurrentMapConfiguration(new PropertiesConfiguration(fileName));
        } catch (ConfigurationException e) {
            e.printStackTrace();
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
