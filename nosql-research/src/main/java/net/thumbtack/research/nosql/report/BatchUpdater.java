package net.thumbtack.research.nosql.report;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class provides buffering for frequent and probably duplicating events which need to be saved to some storage.
 * You can create several buffers of the same type, e.g. one buffer per shard, or per event type.
 * It accumulates them until buffer is less then bufferSize (num of records) and then flushes records to storage.
 * Also, in separate thread buffers are flushed periodically, every flushInterval (in ms).
 * If events have the same hashCode, newer events overwrite older one (in buffer), if they hasn't been flushed yet.
 *
 * @param <T> type of the event objects to buffer
 */
public class BatchUpdater<T> {

	private static final Logger logger = LoggerFactory.getLogger("BatchUpdater");

	public static final int DEFAULT_BUFFER_SIZE = 20;
	public static final int DEFAULT_FLUSH_INTERVAL = 1000;

	private static List<BatchUpdater> instances = new ArrayList<BatchUpdater>();

	private Map<Integer, Set<T>> eventBuffers;
	private Map<Integer, FlushEvent<T>> events;
	private Map<Integer, ExecutorService> executors;
	private Timer flushTimer;
	private int bufferSize = 0;
	private String name;

	/**
	 * Create new batch event updater
	 * @param name identifier of updater
	 * @param bufferSize maximum size of buffer (when reached buffer is flushed)
	 * @param flushInterval interval to sue for periodical buffer flushing
	 */
	public BatchUpdater(String name, int bufferSize) {
		this.name = name;
		this.bufferSize = bufferSize;

		eventBuffers = new ConcurrentHashMap<Integer, Set<T>>();
		events = new ConcurrentHashMap<Integer, FlushEvent<T>>();
		executors = new ConcurrentHashMap<Integer, ExecutorService>();

		// register new instance
		synchronized (instances) {
			instances.add(this);
		}
    }

    public void startFlushTimer(final int flushInterval) {
        // create and run periodical flusher thread
        if (flushInterval > 0) {
            TimerTask task = new TimerTask() {
                @Override
                public void run() {
                    for (int id : executors.keySet()) {
                        executors.get(id).execute(new ExecutorTask(id));
                    }
                }
            };
            flushTimer = new Timer();
            flushTimer.schedule(task, flushInterval, flushInterval);
        }
    }

    /**
	 * Flush buffers and shutdown all executors
	 */
	public void cleanup() {

		// try to flush all buffers first
		try {
			for(int bufferId : eventBuffers.keySet()) {
				flush(bufferId);
			}
		}
		finally {
			// stop flush timer
			if (flushTimer != null) {
				flushTimer.cancel();
			}

			// shutdown all flush executors
			for (int id : executors.keySet()) {
				executors.get(id).shutdown();
			}
		}
	}

	/**
	 * Add new event flusher object for selected buffer
	 * @param bufferId id of buffer to use
	 * @param event callback for flush object instance to execute actual buffer flush
	 */
	public void addEvent(int bufferId, FlushEvent<T> event) {
		eventBuffers.put(bufferId, new HashSet<T>(bufferSize));
		events.put(bufferId, event);
		executors.put(bufferId, Executors.newSingleThreadExecutor(new NamedThreadFactory("batchUpdater-" + name)));
	}

	/**
	 * Get all buffered records from specified buffer
	 * @param bufferId id of buffer to use
	 * @return list of event objects in specified buffer
	 */
	public List<T> getAll(final int bufferId) {
		final Set<T> eventBuffer = eventBuffers.get(bufferId);
		List<T> list;
		synchronized (eventBuffer) {
			list = new ArrayList<T>(eventBuffer);
		}
		return list;
	}

	/**
	 * Add new object to selected buffer
	 * @param bufferId id of buffer to use
	 * @param o object to add into selected buffer
	 */
	public void add(final int bufferId, T o) {
		final Set<T> eventBuffer = eventBuffers.get(bufferId);

		// overwrite existing event with new one
		synchronized (eventBuffer) {
			eventBuffer.add(o);
		}

		// queueing disabled
		if (bufferSize <= 0) {
			flush(bufferId);
		}
		else {
			// queue is full
			if (eventBuffer.size() >= bufferSize) {
				executors.get(bufferId).execute(new ExecutorTask(bufferId));
			}
		}
	}

	/**
	 * Force flushing of selected buffer
	 * @param bufferId id of buffer to use
	 */
	public void flush(int bufferId) {
		List<T> buffer;
		Set<T> eventBuffer = eventBuffers.get(bufferId);
		synchronized (eventBuffer) {
			buffer = new ArrayList<T>(eventBuffer);
			eventBuffer.clear();
		}

//		if (! buffer.isEmpty()) {
			events.get(bufferId).flush(buffer);
//		}
	}

	/**
	 * Flush all buffers
	 */
	public void flush() {
		for(int bufferId : eventBuffers.keySet()) {
			flush(bufferId);
		}
	}

	/**
	 * Flush all buffers in all instance of BatchUpdater and its successors
	 */
	public static void flushAll() {
		synchronized (instances) {
			for(BatchUpdater bu : instances) {
				bu.flush();
			}
		}
	}

	/**
	 * This interface represents callback for flushing batched events.
	 * It should provide the only flush method for doing event-specific batch processing,
	 * like writing new records into database or sending buffered messages.
	 * @param <T> type of event objects to be flushed
	 */
	public interface FlushEvent<T> {

		/**
		 * Event-specific flush methods
		 * @param buffer values to flush
		 */
		void flush(Collection<T> buffer);
	}

	private class ExecutorTask implements Runnable {

		private int id;

		public ExecutorTask(int id) {
			this.id = id;
		}

		@Override
		public void run() {
			flush(id);
		}
	}

}