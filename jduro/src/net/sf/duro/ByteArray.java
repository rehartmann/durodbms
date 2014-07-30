package net.sf.duro;

import java.io.Serializable;

/**
 * Instances of this class represent resizable byte arrays.
 * @author Rene Hartmann
 *
 */
public class ByteArray implements Serializable {
    private static final long serialVersionUID = 1L;

    private byte[] bytes;

    public ByteArray() {
	bytes = null;
    }

    public ByteArray(byte[] bytes) {
	this.bytes = bytes;
    }

    public int getLength() {
	return bytes.length;
    }

    public void setLength(int l) {
	byte[] b = new byte[l];
	System.arraycopy(bytes, 0, b, 0, l < bytes.length ? l : bytes.length);
	bytes = b;
    }

    public byte[] getBytes() {
	return bytes;
    }

    public void setBytes(byte[] bytes) {
	this.bytes = bytes;
    }
}
