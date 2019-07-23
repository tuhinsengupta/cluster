package org.tuhin.cluster.security;

import java.security.NoSuchAlgorithmException;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

public class SymmetricKey {
	private long   genTime;
	private String algo;
	private SecretKey skey;
	
	public SymmetricKey(String algo) throws NoSuchAlgorithmException {
		this.algo = algo;
		genkey();
	}
	
	public void genkey() throws NoSuchAlgorithmException {
		KeyGenerator kgen = KeyGenerator.getInstance(algo);
		skey = kgen.generateKey();
		genTime = System.currentTimeMillis();
	}
	
	public SecretKey getSecretKey() {
		return skey;
	}
	
	public boolean isExpired(long expiryTimeInMillis) {
		long currentTime = System.currentTimeMillis();
		return ( currentTime - genTime ) >= expiryTimeInMillis;
	}

}
