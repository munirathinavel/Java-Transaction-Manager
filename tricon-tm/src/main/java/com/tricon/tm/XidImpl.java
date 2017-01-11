package com.tricon.tm;

import java.io.Serializable;
import java.util.Arrays;

import javax.transaction.xa.Xid;

import com.tricon.tm.util.EncodingUtil;

/**
 * Xid implementation for JTA
 * 
 * <pre>
 * XID has the following format as defined by X/Open Specification:
 * 
 *     XID
 *         long formatId              format identifier
 *         long gtrid_length          value 1-64
 *         long bqual_length          value 1-64
 *         byte data [XIDDATASIZE]    where XIDDATASIZE = 128
 * 
 *     The data field comprises at most two contiguous components:
 *     a global transaction identifier (gtrid) and a branch qualifier (bqual)
 *     which are defined as:
 * 
 *         byte gtrid [1-64]          global transaction identfier
 *         byte bqual [1-64]          branch qualifier
 * </pre>
 */
public class XidImpl implements Xid, Serializable {
	private static final long serialVersionUID = 3302591392416605570L;

	/**
	 * Constant to hold the int-encoded "Trcn" string. This is globally unique ID to represent Tricon XIDs
	 */
	public static final int TRICON_FORMAT_ID = 0x5472636e;

	private int formatId;
	private byte[] globalTransactionId;
	private int globalTransactionIdLength;
	private byte[] branchQualifier;
	private int branchQualifierLength;

	public XidImpl(byte[] globalTransactionId, byte[] branchQualifier) {
		this(TRICON_FORMAT_ID, globalTransactionId, branchQualifier);
	}

	public XidImpl(int formatId, byte[] globalTransactionId, byte[] branchQualifier) {
		this.formatId = formatId;
		this.globalTransactionId = globalTransactionId;
		this.globalTransactionIdLength = globalTransactionId != null ? globalTransactionId.length : 0;
		this.branchQualifier = branchQualifier;
		this.branchQualifierLength = branchQualifier != null ? branchQualifier.length : 0;
	}

	public int getFormatId() {
		return formatId;
	}

	public byte[] getGlobalTransactionId() {
		return globalTransactionId;
	}

	public byte[] getBranchQualifier() {
		return branchQualifier;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + formatId;
		result = prime * result + Arrays.hashCode(globalTransactionId);
		result = prime * result + Arrays.hashCode(branchQualifier);
		return result;
	}

	@Override
	public boolean equals(Object obj2) {
		if (obj2 == null || !(obj2 instanceof XidImpl)) {
			return false;
		}
		XidImpl xid2 = (XidImpl) obj2;
		if (formatId == xid2.getFormatId()
				&& Arrays.equals(globalTransactionId, xid2.getGlobalTransactionId())
				&& Arrays.equals(branchQualifier, xid2.getBranchQualifier())) {
			return true;
		}
		return false;
	}

	@Override
	public String toString() {
		return new StringBuffer(this.getClass().getName()).append("[")
				.append(EncodingUtil.convertHexToString(EncodingUtil.convertIntToHexString(formatId)))
				.append(":").append(globalTransactionIdLength)
				.append(":").append(branchQualifierLength)
				.append(":").append(EncodingUtil.convertBytesToHex(globalTransactionId))
				.append(":").append(EncodingUtil.convertBytesToHex(branchQualifier))
				.append("]").toString();
	}

}
