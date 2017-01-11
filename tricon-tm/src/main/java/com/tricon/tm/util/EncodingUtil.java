package com.tricon.tm.util;

public final class EncodingUtil {
	private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

	private EncodingUtil() {
	}

	public static String convertStringToHex(String input) {
		if (input == null) {
			throw new NullPointerException();
		}
		return convertBytesToHex(input.getBytes());
	}

	public static String convertHexToString(String txtInHex) {
		byte[] txtInByte = new byte[txtInHex.length() / 2];
		int j = 0;
		for (int i = 0; i < txtInHex.length(); i += 2) {
			txtInByte[j++] = Byte.parseByte(txtInHex.substring(i, i + 2), 16);
		}
		return new String(txtInByte);
	}

	public static String convertIntToHexString(int val) {
		return Integer.toHexString(val);
	}

	public static String convertBytesToHex(byte[] buf) {
		if (buf != null) {
			char[] chars = new char[2 * buf.length];
			for (int i = 0; i < buf.length; ++i) {
				chars[2 * i] = HEX_CHARS[(buf[i] & 0xF0) >>> 4];
				chars[2 * i + 1] = HEX_CHARS[buf[i] & 0x0F];
			}
			return new String(chars);
		}
		return null;
	}

	public static byte[] convertLongToBytes(long aLong) {
		byte[] array = new byte[8];

		array[7] = (byte) (aLong & 0xff);
		array[6] = (byte) ((aLong >> 8) & 0xff);
		array[5] = (byte) ((aLong >> 16) & 0xff);
		array[4] = (byte) ((aLong >> 24) & 0xff);
		array[3] = (byte) ((aLong >> 32) & 0xff);
		array[2] = (byte) ((aLong >> 40) & 0xff);
		array[1] = (byte) ((aLong >> 48) & 0xff);
		array[0] = (byte) ((aLong >> 56) & 0xff);

		return array;
	}

	public static byte[] convertIntToBytes(int anInt) {
		byte[] array = new byte[4];

		array[3] = (byte) (anInt & 0xff);
		array[2] = (byte) ((anInt >> 8) & 0xff);
		array[1] = (byte) ((anInt >> 16) & 0xff);
		array[0] = (byte) ((anInt >> 24) & 0xff);

		return array;
	}

	public static byte[] convertShortToBytes(short aShort) {
		byte[] array = new byte[2];

		array[1] = (byte) (aShort & 0xff);
		array[0] = (byte) ((aShort >> 8) & 0xff);

		return array;
	}

	public static long convertBytesToLong(byte[] bytes, int pos) {
		if (bytes.length + pos < 8) {
			throw new IllegalArgumentException("a long can only be decoded from 8 bytes of an array (got a "
					+ bytes.length + " byte(s) array, must start at position " + pos + ")");
		}
		long result = 0;
		for (int i = 0; i < 8; i++) {
			result <<= 8;
			result ^= (long) bytes[i + pos] & 0xFF;
		}
		return result;
	}

	public static int convertBytesToInt(byte[] bytes, int pos) {
		if (bytes.length + pos < 4) {
			throw new IllegalArgumentException("an integer can only be decoded from 4 bytes of an array (got a "
					+ bytes.length + " byte(s) array, must start at position " + pos + ")");
		}
		int result = 0;
		for (int i = 0; i < 4; i++) {
			result <<= 8;
			result ^= (int) bytes[i + pos] & 0xFF;
		}
		return result;
	}

}
