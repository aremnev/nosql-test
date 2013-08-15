package net.thumbtack.research.nosql.scenarios;

import java.util.HashMap;
import java.util.Map;

/**
 * User: vkornev
 * Date: 13.08.13
 * Time: 10:46
 */
public final class ScenarioPool {
    public static final String SC_CONSISTENCY_A = "consistency_a";
    public static final String SC_CONSISTENCY_B = "consistency_b";
    public static final String SC_CONSISTENCY_C = "consistency_c";

    private static final ScenarioPool instance = new ScenarioPool();

    private final Map<String, Class<? extends Scenario>> databasePool;

    private ScenarioPool() {
        databasePool = new HashMap<>();
        databasePool.put(SC_CONSISTENCY_A, ConsistencyAScenario.class);
        databasePool.put(SC_CONSISTENCY_B, ConsistencyBScenario.class);
        databasePool.put(SC_CONSISTENCY_C, ConsistencyCScenario.class);
    }

    public static Scenario get(String scenarioName) throws IllegalAccessException, InstantiationException {
        return instance.databasePool.get(scenarioName).newInstance();
    }
}
