package com.tricon.tm.internal;

import java.util.Date;
import java.util.Iterator;
import java.util.List;

import javax.transaction.xa.XAResource;

import com.tricon.tm.XidImpl;

public final class XAResourceHelper {

	public static XAResourceInfo createXAResourceInfo(XAResource xaResource,
			XidImpl xid, Date transactionTimeoutDate) {
		XAResourceInfo xaResourceInfo = new XAResourceInfo(xaResource, xid, transactionTimeoutDate);

		return xaResourceInfo;
	}
	
	@SuppressWarnings("rawtypes")
	public static String getXAResourceInfosString(final List<XAResourceInfo> resources) {
		final StringBuffer sb = new StringBuffer("[");
		Iterator it = resources.iterator();
		while (it.hasNext()) {
			XAResourceInfo xaResourceInfo = (XAResourceInfo) it.next();
			sb.append(xaResourceInfo.toString());
			if (it.hasNext()) {
				sb.append(", ");
			}
		}
		return sb.append("]").toString();
	}

}
