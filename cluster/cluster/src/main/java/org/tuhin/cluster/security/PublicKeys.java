package org.tuhin.cluster.security;

import java.io.Serializable;

public class PublicKeys implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7145332907993759560L;
	private byte[] RSAPublicKey;
	private byte[] DSAPublicKey;
	public byte[] getRSAPublicKey() {
		return RSAPublicKey;
	}
	public byte[] getDSAPublicKey() {
		return DSAPublicKey;
	}
	public PublicKeys(byte[] RSAPublicKey, byte[] DSAPublicKey) {
		super();
		this.RSAPublicKey = RSAPublicKey;
		this.DSAPublicKey = DSAPublicKey;
	}
	
	
}
