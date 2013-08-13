package net.thumbtack.research.nosql.scenarios;

import java.util.HashMap;
import java.util.Map;

/**
 * User: vkornev
 * Date: 13.08.13
 * Time: 10:46
 */
public class ScenarioPool {
    private static final Map<String, Class<? extends Scenario>> databasePool = new HashMap<String, Class<? extends Scenario>>();
    private static final String SC_CONSISTENCY = "consistency";
    private static final String[] scenarios = {SC_CONSISTENCY};

    private static final ScenarioPool instance = new ScenarioPool();

    @SuppressWarnings("unchecked")
    private ScenarioPool() {
        databasePool.put(SC_CONSISTENCY, ConsistencyScenario.class);
    }

    public static Scenario get(String scenarioName) throws IllegalAccessException, InstantiationException {
        return databasePool.get(scenarioName).newInstance();
    }
}
