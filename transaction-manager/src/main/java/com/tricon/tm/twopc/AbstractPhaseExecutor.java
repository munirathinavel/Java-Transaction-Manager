package com.tricon.tm.twopc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tricon.tm.internal.exception.TriconRuntimeException;
import com.tricon.tm.internal.XAResourceInfo;
import com.tricon.tm.internal.XAResourceManager;
import com.tricon.tm.util.DecodingUtil;

public abstract class AbstractPhaseExecutor {
	private static Logger logger = LoggerFactory.getLogger(AbstractPhaseExecutor.class);

	private ExecutorService executorService;

	public AbstractPhaseExecutor(ExecutorService executorService) {
		this.executorService = executorService;
	}

	protected void executePhase(final XAResourceManager resourceManager) throws PhaseException {
		JobExecutionResult report = createAndExecuteJobs(resourceManager.getAllXAResourceInfoList());
		if (report.getResourceExceptionMap().size() > 0) {
			throw new PhaseException(report.getResourceExceptionMap());
		}
	}

	protected abstract Job createJob(XAResourceInfo xaResourceInfo);

	protected abstract boolean isParticipating(XAResourceInfo xaResourceInfo);

	@SuppressWarnings("rawtypes")
	protected void logFailedResources(PhaseException ex) {
		logger.debug("Logging failed resoures {}", "..");
		Iterator itr = ex.getResourceExceptionMap().entrySet().iterator();
		while (itr.hasNext()) {
			Map.Entry mapEntry = (Map.Entry) itr.next();
			XAResourceInfo xaResourceInfo = (XAResourceInfo) mapEntry.getKey();
			Throwable throwable = (Throwable) mapEntry.getValue();
			logger.error("Resource {} failed on transaction {} - ex: {}",
					new Object[] { xaResourceInfo, xaResourceInfo.getXid(), throwable });
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private JobExecutionResult createAndExecuteJobs(final List<XAResourceInfo> resources) {
		final List jobs = new ArrayList();
		final Map resourceExceptionMap = new LinkedHashMap();

		// add jobs for execution by starting the threads
		for (final XAResourceInfo xaResourceInfo : resources) {
			if (!isParticipating(xaResourceInfo)) {
				logger.debug("Skipping non-participating resource {}", xaResourceInfo);
				continue;
			}
			Job job = createJob(xaResourceInfo);
			Future<?> future = executorService.submit(job);
			job.setFuture(future);
			jobs.add(job);
		}

		// wait for threads to finish all the jobs and check results
		for (int i = 0; i < jobs.size(); i++) {
			final Job job = (Job) jobs.get(i);
			Future<?> future = job.getFuture();
			while (!future.isDone()) {
				try {
					future.get(1000l, TimeUnit.MILLISECONDS);
				} catch (InterruptedException ie) {
					logger.error("InterruptedException while calling get() on Future: {} - ex: {}", future, ie);
					throw new TriconRuntimeException("Error calling get() on Future: " + future + " - ex: ", ie);
				} catch (ExecutionException ee) {
					logger.error("ExecutionException while calling get() on Future: {} - ex: {}", future, ee);
					throw new TriconRuntimeException("Error calling get() on Future: " + future + " - ex: ", ee);
				} catch (TimeoutException te) {
					logger.error("TimeoutException while calling get() on Future: {} - ex: {}", future, te);
					// Ignore exception
				}
			}
			populateResourceExceptionMap(job, resourceExceptionMap);
		}
		logger.debug("Phase executed with {} exception(s)", resourceExceptionMap.size());
		return new JobExecutionResult(resourceExceptionMap);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void populateResourceExceptionMap(final Job job, final Map resourceExceptionMap) {
		if (job.getXAException() != null) {
			logger.error("Error while executing job: {}, errorCode={}",
					job, DecodingUtil.decodeXAExceptionErrorCode(job.getXAException()));
			resourceExceptionMap.put(job.getXAResourceInfo(), job.getXAException());

		} else if (job.getRuntimeException() != null) {
			logger.error("Error while executing job: {}", job);
			resourceExceptionMap.put(job.getXAResourceInfo(), job.getRuntimeException());
		}
	}

	private static final class JobExecutionResult {
		private Map resourceExceptionMap;

		private JobExecutionResult(Map resourceExceptionMap) {
			this.resourceExceptionMap = resourceExceptionMap;
		}

		public Map getResourceExceptionMap() {
			return resourceExceptionMap;
		}

		public void setResourceExceptionMap(Map resourceExceptionMap) {
			this.resourceExceptionMap = resourceExceptionMap;
		}
	}

}
