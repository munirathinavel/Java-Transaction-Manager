package com.tricon.tm.internal;

import java.io.Serializable;
import java.util.Date;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tricon.tm.XidImpl;
import com.tricon.tm.internal.exception.TriconXAException;
import com.tricon.tm.util.DecodingUtil;

public class XAResourceInfo implements Serializable {
	private static final long serialVersionUID = 3166057561965872075L;

	private static Logger logger = LoggerFactory.getLogger(XAResourceInfo.class);

	private XidImpl xid;
	private XAResource xaResource;
	private Date transactionTimeoutDate;
	private boolean isTimeoutAlreadySet;

	private boolean started;
	private boolean ended;
	private boolean suspended;
	private boolean failed;

	public XAResourceInfo(XAResource xaResource) {
		this(xaResource, null, null);
	}

	public XAResourceInfo(XAResource xaResource, XidImpl xid) {
		this(xaResource, xid, null);
	}

	public XAResourceInfo(XAResource xaResource, XidImpl xid, Date transactionTimeoutDate) {
		this.xaResource = xaResource;
		this.xid = xid;
		this.transactionTimeoutDate = transactionTimeoutDate;
		isTimeoutAlreadySet = false;

		started = false;
		ended = false;
		suspended = false;
		failed = false;
	}

	public XidImpl getXid() {
		return xid;
	}

	public void setXid(XidImpl xid) {
		this.xid = xid;
	}

	public XAResource getXAResource() {
		return xaResource;
	}

	public void setXAResource(XAResource xaResource) {
		this.xaResource = xaResource;
	}

	public Date getTransactionTimeoutDate() {
		return transactionTimeoutDate;
	}

	public void setTransactionTimeoutDate(Date transactionTimeoutDate) {
		this.transactionTimeoutDate = transactionTimeoutDate;
	}

	public boolean isTimeoutAlreadySet() {
		return isTimeoutAlreadySet;
	}

	public void setTimeoutAlreadySet(boolean isTimeoutAlreadySet) {
		this.isTimeoutAlreadySet = isTimeoutAlreadySet;
	}

	public boolean isStarted() {
		return started;
	}

	public void setStarted(boolean started) {
		this.started = started;
	}

	public boolean isEnded() {
		return ended;
	}

	public void setEnded(boolean ended) {
		this.ended = ended;
	}

	public boolean isSuspended() {
		return suspended;
	}

	public void setSuspended(boolean suspended) {
		this.suspended = suspended;
	}

	public boolean isFailed() {
		return failed;
	}

	public void setFailed(boolean failed) {
		this.failed = failed;
	}

	public void start(int flag) throws XAException {
		boolean suspended = this.suspended;
		boolean started = this.started;

		if (this.ended && (flag == XAResource.TMRESUME)) {
			logger.debug("Resource already ended, changing state to resumed: {}", this);
			this.suspended = false;
			return;
		}

		if (flag == XAResource.TMRESUME) {
			if (!this.suspended) {
				throw new TriconXAException("Resource hasn't been suspended, cannot resume it: "
						+ this, XAException.XAER_PROTO);
			}
			if (!this.started) {
				throw new TriconXAException("Resource hasn't been started, cannot resume it: "
						+ this, XAException.XAER_PROTO);
			}
			logger.debug("Resuming resource: {} with {}", this, DecodingUtil.decodeXAResourceFlag(flag));
			suspended = false;

		} else {
			if (this.started) {
				throw new TriconXAException("Resource already started: " + this, XAException.XAER_PROTO);
			}
			logger.debug("Starting resource: {} with {}", this, DecodingUtil.decodeXAResourceFlag(flag));
			started = true;
		}

		boolean applyTransactionTimeout = true; // Need to take dynamically
		if (!isTimeoutAlreadySet && transactionTimeoutDate != null && applyTransactionTimeout) {
			int timeoutInSeconds = (int) ((transactionTimeoutDate.getTime() - System.currentTimeMillis() + 999L) / 1000L);
			// setting a timeout of 0 means resetting -> set it to at least 1
			timeoutInSeconds = Math.max(1, timeoutInSeconds);
			logger.debug("Applying resource timeout of {} seconds on {}", timeoutInSeconds, this);
			getXAResource().setTransactionTimeout(timeoutInSeconds);
			isTimeoutAlreadySet = true;
		}
		try {
			getXAResource().start(xid, flag);
			logger.debug("Started resource: {} with {}", this, DecodingUtil.decodeXAResourceFlag(flag));
		} catch (XAException ex) {
			failed = true;
			throw ex;
		} finally {
			this.suspended = suspended;
			this.started = started;
			this.ended = false;
		}
	}

	public void end(int flag) throws XAException {
		boolean ended = this.ended;
		boolean suspended = this.suspended;

		if (this.ended && (flag == XAResource.TMSUSPEND)) {
			logger.debug("Resource already ended, changing state to suspended: {}", this);
			this.suspended = true;
			return;
		}

		if (this.ended) {
			throw new TriconXAException("Resource already ended: " + this, XAException.XAER_PROTO);
		}

		if (flag == XAResource.TMSUSPEND) {
			if (!this.started) {
				throw new TriconXAException("Resource hasn't been started, cannot suspend it: "
						+ this, XAException.XAER_PROTO);
			}
			if (this.suspended) {
				throw new TriconXAException("Resource already suspended: " + this, XAException.XAER_PROTO);
			}
			logger.debug("Suspending resource: {} with {}", this, DecodingUtil.decodeXAResourceFlag(flag));
			suspended = true;

		} else {
			logger.debug("Ending resource: {} with {}", this, DecodingUtil.decodeXAResourceFlag(flag));
			ended = true;
		}

		try {
			getXAResource().end(xid, flag);
			logger.debug("Ended resource: {} with {}", this, DecodingUtil.decodeXAResourceFlag(flag));
		} catch (XAException ex) {
			failed = true;
			throw ex;
		} finally {
			this.suspended = suspended;
			this.ended = ended;
			this.started = false;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + getXAResource().hashCode();
		if (xid != null) {
			result = prime * result + xid.hashCode();
		}
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof XAResourceInfo)) {
			return false;
		}
		XAResourceInfo other = (XAResourceInfo) obj;
		return equals(other.getXAResource(), getXAResource()) && equals(other.xid, xid);
	}

	private boolean equals(Object obj1, Object obj2) {
		if (obj1 == obj2) {
			return true;
		}
		if (obj1 == null || obj2 == null) {
			return false;
		}
		return obj1.equals(obj2);
	}

	@Override
	public String toString() {
		return new StringBuffer(this.getClass().getName()).append("[")
				.append("xid=").append(xid)
				.append(", xaResource=").append(xaResource)
				.append(", started=").append(started)
				.append(", ended=").append(ended)
				.append(", suspended=").append(suspended)
				.append(", failed=").append(failed)
				.append("]").toString();
	}

}
