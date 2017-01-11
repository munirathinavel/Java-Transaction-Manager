package com.tricon.tm;

import java.io.Serializable;
import java.util.Arrays;

import com.tricon.tm.util.EncodingUtil;

public class SyncTransactionKey implements Serializable {
	private static final long serialVersionUID = -7175830375492153165L;

	private byte[] globalTransactionId;

	public SyncTransactionKey(byte[] globalTransactionId) {
		this.globalTransactionId = globalTransactionId;
	}

	public byte[] getGlobalTransactionId() {
		return globalTransactionId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(globalTransactionId);
		return result;
	}

	@Override
	public boolean equals(Object obj2) {
		if (obj2 == null || !(obj2 instanceof SyncTransactionKey)) {
			return false;
		}
		SyncTransactionKey syncTransKey2 = (SyncTransactionKey) obj2;
		return Arrays.equals(globalTransactionId, syncTransKey2.getGlobalTransactionId());
	}

	@Override
	public String toString() {
		return new StringBuffer(this.getClass().getName()).append("[")
				.append(":").append(EncodingUtil.convertBytesToHex(globalTransactionId))
				.append("]").toString();
	}

}
