package com.tricon.tm.util;

import java.util.concurrent.atomic.AtomicLong;

import com.tricon.tm.TriconTransactionManagerServices;
import com.tricon.tm.XidImpl;

public final class XidUtil {
	private static AtomicLong sequenceNumber = new AtomicLong(0l);

	private XidUtil() {
	}

	/**
	 * Creates new XidImpl using provided global transaction id. It sets format id and new branch qualifier.
	 * 
	 * @param globalTransactionId
	 * @return
	 */
	public static XidImpl createXid(byte[] globalTransactionId) {
		return new XidImpl(globalTransactionId, generateUniqueXidDataComponent());
	}

	/**
	 * Genenares unique Xid's 'data' component (global transaction id or branch qualifier)
	 * 
	 * @return
	 */
	public static byte[] generateUniqueXidDataComponent() {
		// Transaction manager vendor name - fixed 6 bytes
		byte[] tmVendorName = TriconTransactionManagerServices.getConfigurationHelper().buildTMVendorNameByteArray();
		// Server id - fixed 42 bytes
		byte[] serverId = TriconTransactionManagerServices.getConfigurationHelper().buildServerIdByteArray();
		// Timestamp - fixed 8 bytes
		byte[] timestamp = EncodingUtil.convertLongToBytes(System.currentTimeMillis());
		// Sequence number - fixed 8 bytes
		byte[] sequenceNo = EncodingUtil.convertLongToBytes(getNextSequenceNumber());

		int uidLength = tmVendorName.length + serverId.length + timestamp.length + sequenceNo.length;
		byte[] uidArray = new byte[uidLength];

		System.arraycopy(tmVendorName, 0, uidArray, 0, tmVendorName.length);
		System.arraycopy(serverId, 0, uidArray, tmVendorName.length, serverId.length);
		System.arraycopy(timestamp, 0, uidArray, tmVendorName.length + serverId.length, timestamp.length);
		System.arraycopy(sequenceNo, 0, uidArray, tmVendorName.length + serverId.length + timestamp.length, sequenceNo.length);

		return uidArray;
	}

	public static long getNextSequenceNumber() {
		return sequenceNumber.incrementAndGet();
	}

}
