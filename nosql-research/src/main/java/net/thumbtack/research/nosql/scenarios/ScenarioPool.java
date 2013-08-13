package net.thumbtack.research.nosql.scenarios;

import java.util.HashMap;
import java.util.Map;

/**
 * User: vkornev
 * Date: 13.08.13
 * Time: 10:46
 */
public class ScenarioPool {
    private static final String SC_CONSISTENCY = "consistency";
    private static final String[] scenarios = {SC_CONSISTENCY};
    private static final ScenarioPool instance = new ScenarioPool();

    private final Map<String, Class<? extends Scenario>> databasePool;

    @SuppressWarnings("unchecked")
    private ScenarioPool() {
        databasePool = new HashMap<String, Class<? extends Scenario>>();
        databasePool.put(SC_CONSISTENCY, ConsistencyScenario.class);
    }

    public static Scenario get(String scenarioName) throws IllegalAccessException, InstantiationException {
        return instance.databasePool.get(scenarioName).newInstance();
    }
}
