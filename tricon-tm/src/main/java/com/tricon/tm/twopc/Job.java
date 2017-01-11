package com.tricon.tm.twopc;

import java.util.concurrent.Future;

import javax.transaction.xa.XAException;

import com.tricon.tm.TriconTransactionManagerServices;
import com.tricon.tm.internal.XAResourceInfo;

public abstract class Job implements Runnable {
	private Future future;
	private XAResourceInfo xaResourceInfo;

	protected XAException xaException;
	protected RuntimeException runtimeException;

	public Job(XAResourceInfo xaResourceInfo) {
		this.xaResourceInfo = xaResourceInfo;
	}

	public Future getFuture() {
		return future;
	}

	public void setFuture(Future future) {
		this.future = future;
	}

	public XAResourceInfo getXAResourceInfo() {
		return xaResourceInfo;
	}

	public void setXAResourceInfo(XAResourceInfo xaResourceInfo) {
		this.xaResourceInfo = xaResourceInfo;
	}

	public XAException getXAException() {
		return xaException;
	}

	public void setXAException(XAException xaException) {
		this.xaException = xaException;
	}

	public RuntimeException getRuntimeException() {
		return runtimeException;
	}

	public void setRuntimeException(RuntimeException runtimeException) {
		this.runtimeException = runtimeException;
	}

	public final void run() {
		if (TriconTransactionManagerServices.getConfigurationHelper().isAsynchronous2pc()) {
			Thread.currentThread().setName("tricon-tm-2pc-async-" + Thread.currentThread().getId());
		} else {
			Thread.currentThread().setName("tricon-tm-2pc-sync-" + Thread.currentThread().getId());
		}
		execute();
	}

	protected abstract void execute();

}
