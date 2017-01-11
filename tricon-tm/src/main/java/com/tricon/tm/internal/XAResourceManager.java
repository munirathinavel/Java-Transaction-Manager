package com.tricon.tm.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tricon.tm.TriconTransactionManagerServices;
import com.tricon.tm.XidImpl;
import com.tricon.tm.internal.exception.TriconSystemException;
import com.tricon.tm.internal.exception.TriconXAException;
import com.tricon.tm.util.XidUtil;

public class XAResourceManager {
	private static Logger logger = LoggerFactory.getLogger(XAResourceManager.class);

	private byte[] globalTransacationId;
	private final List<XAResourceInfo> resources = new ArrayList<XAResourceInfo>();

	public XAResourceManager(byte[] globalTransacationId) {
		this.globalTransacationId = globalTransacationId;
	}

	public byte[] getGlobalTransactionId() {
		return globalTransacationId;
	}

	public int resourceCount() {
		return resources.size();
	}

	public void enlist(XAResourceInfo xaResourceInfo) throws XAException, TriconSystemException {
		logger.debug("Enlisting resource: {} ..", xaResourceInfo);
		final XAResourceInfo enlistedXAResourceInfo = findEnlistedXAResourceInfo(xaResourceInfo.getXAResource());
		if (enlistedXAResourceInfo != null && !enlistedXAResourceInfo.isEnded()) {
			xaResourceInfo.setXid(enlistedXAResourceInfo.getXid());
			logger.warn("Ignoring enlistment of already enlisted but not ended resource {}", xaResourceInfo);
			return;
		}

		final XAResourceInfo toBeJoinedXAResourceInfo = getEnlistedXAResourceInfoToBeJoined(xaResourceInfo);

		startOrJoinTransactionBranch(xaResourceInfo, toBeJoinedXAResourceInfo);
	}

	private XAResourceInfo getEnlistedXAResourceInfoToBeJoined(final XAResourceInfo xaResourceInfo) throws XAException {
		boolean isUseTMJoin = TriconTransactionManagerServices.getConfigurationHelper().isUseTMJoin();
		if (isUseTMJoin) {
			logger.debug("Checking for existing transaction branch joinability {}", "..");
			Iterator<XAResourceInfo> iter = resources.iterator();
			while (iter.hasNext()) {
				XAResourceInfo enlistedXAResourceInfo = iter.next();
				if (enlistedXAResourceInfo.isEnded() && !enlistedXAResourceInfo.isSuspended() &&
						xaResourceInfo.getXAResource().isSameRM(enlistedXAResourceInfo.getXAResource())) {
					logger.debug("Joinable (enlisted, ended and belongs to the same RM) enlistedXAResourceInfo: {}",
							enlistedXAResourceInfo);
					return enlistedXAResourceInfo;
				}
			}
			logger.debug("Joinable (enlisted, ended and belongs to the same RM) enlistedXAResourceInfo: {}", "null");
		}
		return null;
	}

	private void startOrJoinTransactionBranch(final XAResourceInfo xaResourceInfo,
			final XAResourceInfo toBeJoinedXAResourceInfo) throws XAException, TriconSystemException {
		XidImpl xid;
		int flag;

		if (toBeJoinedXAResourceInfo != null) {
			xid = toBeJoinedXAResourceInfo.getXid();
			flag = XAResource.TMJOIN;
			logger.debug("Joining existing transaction branch - xid: {}", xid);

		} else {
			xid = XidUtil.createXid(globalTransacationId);
			flag = XAResource.TMNOFLAGS;
			logger.debug("Creating new transaction branch - xid: {}", xid);
		}

		xaResourceInfo.setXid(xid);
		xaResourceInfo.start(flag);

		// In case of a JOIN, the resource info is already in the list -> do not add it twice
		if (toBeJoinedXAResourceInfo != null) {
			resources.remove(toBeJoinedXAResourceInfo);
		}
		// Add to list only after start() returned successfully
		resources.add(xaResourceInfo);
	}

	public boolean delist(XAResourceInfo xaResourceInfo, int flag) throws XAException, TriconSystemException {
		XAResourceInfo enlistedXAResourceInfo = findEnlistedXAResourceInfo(xaResourceInfo.getXAResource());
		if (enlistedXAResourceInfo != null) {
			logger.debug("Delisting resource: {} ..", xaResourceInfo);
			xaResourceInfo.end(flag);
			return true;
		}
		logger.warn("Trying to delist resource that has not been previously enlisted: {}", xaResourceInfo);
		return false;
	}

	public void suspend() throws XAException {
		Iterator<XAResourceInfo> it = resources.iterator();
		while (it.hasNext()) {
			XAResourceInfo xaResourceInfo = it.next();
			if (!xaResourceInfo.isEnded()) {
				logger.debug("Suspending resource: {} ..", xaResourceInfo);
				xaResourceInfo.end(XAResource.TMSUSPEND);
			}
		}
	}

	public void resume() throws XAException {
		Iterator<XAResourceInfo> it = resources.iterator();
		while (it.hasNext()) {
			XAResourceInfo xaResourceInfo = it.next();
			if (xaResourceInfo.isSuspended()) {
				logger.debug("Resuming suspended resource: {} ..", xaResourceInfo);
				resumeTransactionBranch(xaResourceInfo);

			} else {
				try {
					logger.debug("Re-enlisting resource: {} ..", xaResourceInfo);
					enlist(xaResourceInfo);
				} catch (TriconSystemException ex) {
					throw new TriconXAException("Error while re-enlisting resource during resume: "
							+ xaResourceInfo, XAException.XAER_RMERR, ex);
				}
			}
		}
	}

	private void resumeTransactionBranch(final XAResourceInfo xaResourceInfo) throws XAException {
		XidImpl xid = xaResourceInfo.getXid();
		logger.debug("Resuming transaction branch - xid: {}", xid);
		xaResourceInfo.setXid(xid);
		xaResourceInfo.start(XAResource.TMRESUME);
	}

	public List<XAResourceInfo> getAllXAResourceInfoList() {
		final List<XAResourceInfo> enlistedXaResourceInfoList = new ArrayList<XAResourceInfo>();
		Iterator<XAResourceInfo> iter = resources.iterator();
		while (iter.hasNext()) {
			XAResourceInfo xaResourceInfo = iter.next();
			enlistedXaResourceInfoList.add(xaResourceInfo);
		}
		return enlistedXaResourceInfoList;
	}

	public XAResourceInfo findEnlistedXAResourceInfo(final XAResource xaResource) {
		Iterator<XAResourceInfo> iter = resources.iterator();
		while (iter.hasNext()) {
			XAResourceInfo xaResourceInfo = iter.next();
			if (xaResourceInfo.getXAResource() == xaResource) {
				return xaResourceInfo;
			}
		}
		return null;
	}

	public List<XAResourceInfo> findEnlistedXAResourceInfoListByGtrid(byte[] gtrid) {
		final List<XAResourceInfo> enlistedXaResourceInfoList = new ArrayList<XAResourceInfo>();
		Iterator<XAResourceInfo> iter = resources.iterator();
		while (iter.hasNext()) {
			XAResourceInfo xaResourceInfo = iter.next();
			if (xaResourceInfo.getXid() != null && xaResourceInfo.getXid().getGlobalTransactionId() != null
					&& Arrays.equals(xaResourceInfo.getXid().getGlobalTransactionId(), gtrid)) {
				enlistedXaResourceInfoList.add(xaResourceInfo);
			}
		}
		return enlistedXaResourceInfoList;
	}

	public XAResourceInfo findEnlistedXAResourceInfo(final XAResource xaResource, byte[] gtrid) {
		Iterator<XAResourceInfo> iter = resources.iterator();
		while (iter.hasNext()) {
			XAResourceInfo xaResourceInfo = iter.next();
			if (xaResourceInfo.getXAResource() == xaResource
					&& xaResourceInfo.getXid() != null && xaResourceInfo.getXid().getGlobalTransactionId() != null
					&& Arrays.equals(xaResourceInfo.getXid().getGlobalTransactionId(), gtrid)) {
				return xaResourceInfo;
			}
		}
		return null;
	}

	public void clearXAResourceInfos(byte[] gtrid) {
		Iterator<XAResourceInfo> iter = resources.iterator();
		while (iter.hasNext()) {
			XAResourceInfo xaResourceInfo = iter.next();
			if (xaResourceInfo.getXid() != null && xaResourceInfo.getXid().getGlobalTransactionId() != null
					&& Arrays.equals(xaResourceInfo.getXid().getGlobalTransactionId(), gtrid)) {

				iter.remove();
			}
		}
	}

}
