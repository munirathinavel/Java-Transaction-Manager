package com.tricon.tm.timer;

import com.tricon.tm.TriconTransactionManagerServices;
import com.tricon.tm.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskScheduler extends Thread implements Service {
	private static Logger logger = LoggerFactory.getLogger(TaskScheduler.class);

	private final List tasks = Collections.synchronizedList(new ArrayList());
	private final AtomicBoolean active = new AtomicBoolean(true);

	public TaskScheduler() {
		setName("tricon-tm-task-scheduler");
		setDaemon(true);
		start();
	}

	public void setActive(boolean active) {
		this.active.set(active);
	}

	private boolean isActive() {
		return active.get();
	}

	public int getTasksCount() {
		return tasks.size();
	}

	public void schedule(Task task) {
		addTask(task);
	}

	public <T> boolean cancelByTaskTypeAndObject(Class<T> taskType, final Object object) {
		return removeTaskByTypeAndObject(taskType, object);
	}

	public synchronized void shutdown() {
		logger.info("Shutting down TaskScheduler{}", "..");
		long shutdownInterval = TriconTransactionManagerServices.getConfigurationHelper().getShutdownInterval() * 1000;
		try {
			setActive(false);
			join(shutdownInterval);
		} catch (InterruptedException ex) {
			logger.error("Could not stop the task scheduler within {} seconds", shutdownInterval / 1000);
		}
		logger.info("TaskScheduler is shutdown{}", ".");
	}

	public void run() {
		while (isActive()) {
			try {
				executeTasks();
				Thread.sleep(500l); // execute twice per second
			} catch (InterruptedException ex) {
				// ignore
			}
		}
	}

	@SuppressWarnings("unchecked")
	private boolean addTask(final Task task) {
		synchronized (this.tasks) {
			removeTaskByTypeAndObject(task.getClass(), task.getObject());
			return this.tasks.add(task);
		}
	}

	private <T> boolean removeTaskByTypeAndObject(Class<T> taskType, final Object obj) {
		synchronized (this.tasks) {
			for (int i = 0; i < this.tasks.size(); i++) {
				Task task = (Task) this.tasks.get(i);
				if (task.getClass() == taskType && task.getObject() == obj) {
					this.tasks.remove(task);
					return true;
				}
			}
			return false;
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void executeTasks() {
		if (this.tasks.size() == 0) {
			return;
		}
		List allTasks;
		synchronized (this.tasks) {
			allTasks = new ArrayList(this.tasks);
		}

		Iterator it = allTasks.iterator();
		boolean isExecuteTask = true;
		while (it.hasNext()) {
			final Task task = (Task) it.next();
			if ((task instanceof ScheduledTask)
					&& (!checkTimeoutElapsed(((ScheduledTask) task).getExecutionTime()))) {
				isExecuteTask = false;
			}
			if (isExecuteTask) {
				executeTask(task);
			}
		}
	}

	private void executeTask(final Task task) {
		try {
			task.execute();
		} catch (TaskException ex) {
			logger.error("Error running task: {}, ex: {}", task, ex);
		} finally {
			this.tasks.remove(task);
		}
	}

	private boolean checkTimeoutElapsed(Date executionTime) {
		return (executionTime.compareTo(new Date()) <= 0);
	}

}
