package net.thumbtack.research.nosql.scenarios;

import net.thumbtack.research.nosql.clients.Database;

/**
 * User: vkornev
 * Date: 13.08.13
 * Time: 10:22
 *
 * Base NoSQL database test scenario interface
 */
public abstract class Scenario implements Runnable {
    protected Database db;
    protected long writesCount;

    public Scenario init(Database database, long writesCount) {
        db = database;
        this.writesCount = writesCount;
        return this;
    }

    @Override
    public void run() {
        for (long i=0; i < writesCount; i++) {
            action();
        }
    }

    protected abstract void action();
}
