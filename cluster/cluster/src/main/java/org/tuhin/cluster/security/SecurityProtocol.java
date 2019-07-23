package org.tuhin.cluster.security;

import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.tuhin.cluster.ClusterMember;
import org.tuhin.cluster.ClusterService;

public class SecurityProtocol {

	public static final String ALGO_RSA = "RSA";
	public static final int RSA_KEYSIZE = 2048;
	public static final String ALGO_DSA = "DSA";
	public static final int DSA_KEYSIZE = 512;
	public static final String ALGO_AES = "AES";
	public static final int AES_KEYSIZE = 1024;
	public static final String ALGO_SHA = "SHA1withDSA";
	private static final String CIPHER_RSA = "RSA/ECB/PKCS1Padding";
	private static final String CIPHER_AES = "AES/CBC/PKCS5Padding";
	private static final IvParameterSpec IV    = new IvParameterSpec(new byte[16]);

	public static final byte[] MARKER_ENCRYPTION = {(byte) 0xff};
	public static final byte[] MARKER_NO_ENCRYPTION = {(byte) 0x00};
	private static final long KEY_EXPIRY = 2 * 60 * 1000; // 2 min

	private static SecurityProtocol securityProtocol = null;

	private ClusterService service;
	private SymmetricKey symmetricKey;

	private SecurityProtocol(ClusterService service) throws SecurityException{
		this.service = service;
		try {
			symmetricKey = new SymmetricKey(ALGO_AES);
		} catch (NoSuchAlgorithmException e) {
			throw new SecurityException(e);
		}
	}

	public static void initialize(ClusterService service) throws SecurityException {
		if ( securityProtocol == null ) {
			securityProtocol = new SecurityProtocol(service);
		}
	}
	public static byte[] secure(ClusterMember sendingTo, byte[] plainData) throws SecurityException {
		if (securityProtocol == null ) {
			return createBlock(MARKER_NO_ENCRYPTION, plainData); //No Encryption
		}
		return securityProtocol.encode(sendingTo, plainData);
	}


	public static byte[] unsecure(ClusterMember receivedFrom, byte[] cypherData) throws SecurityException {
		if (securityProtocol == null ) {
			List<byte[]> _blocks = splitBlock(cypherData);

			byte[] marker = _blocks.get(0);

			if ( marker[0] == (byte)0xff ) {
				return _blocks.get(1);
			}else {
				throw new SecurityException("SecurityProtocol not initialized, but secure message received.");
			}
		}
		return securityProtocol.decode(receivedFrom, cypherData);
	}

	private SecretKey generateSymmetricKey() throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException {
		if ( symmetricKey == null ) {
			symmetricKey = new SymmetricKey(ALGO_AES);
		}
		if ( symmetricKey.isExpired(KEY_EXPIRY)) {
			symmetricKey.genkey();
		}
		
		return symmetricKey.getSecretKey();
	}

	private byte[] encode(ClusterMember sendingToMemeber,  byte[] plainData) throws SecurityException {

		if ( sendingToMemeber == null) {
			return createBlock(MARKER_NO_ENCRYPTION, plainData); //No Encryption, e.g. broadcast messages
		}
		PrivateKey signingKey  = service.getCurrent().getDSAPrivateKey();
		PublicKey  encryptKey =  null;
		try {
			encryptKey =  sendingToMemeber.getRSAPublicKey();
		}catch(Exception e) {
			//Ignore
		}

		if (encryptKey == null ) {
			return createBlock(MARKER_NO_ENCRYPTION, plainData); //No Encryption, e.g. first time peer handshake			
		}

		try {
			SecretKey secretKey = generateSymmetricKey();
			byte[] encryptedData = encrypt(plainData,secretKey);
			byte[] encryptedBlock = createBlock(encryptKey(encryptKey,secretKey),encryptedData);

			byte[] signature = sign(signingKey, encryptedBlock);

			byte[] fullBlock = createBlock(signature,encryptedBlock);

			return createBlock(MARKER_ENCRYPTION, fullBlock); //Encryption
		}catch(Exception e) {
			throw new SecurityException(e);

		}
	}

	private byte[] decode(ClusterMember receivedFromMember, byte[] data) throws SecurityException {
	
		List<byte[]> _blocks = splitBlock(data);
	
		byte[] marker = _blocks.get(0);
	
		if ( marker[0] == MARKER_NO_ENCRYPTION[0] ) {
			return _blocks.get(1);
		}
	
		byte[] cypherData = _blocks.get(1);
	
		if (receivedFromMember != null) {
			try {
				PublicKey  signingVerifyKey  = receivedFromMember.getDSAPublicKey();
				PrivateKey  decryptKey         = service.getCurrent().getRSAPrivateKey();
	
				List<byte[]> blocks = splitBlock(cypherData);
	
				boolean dataOk = verify(signingVerifyKey, blocks.get(0), blocks.get(1));
	
				if ( !dataOk) {
					throw new SecurityException("Invalid Signature");
				}
	
				List<byte[]> data_blocks = splitBlock(blocks.get(1));
	
				byte[] encryptedSymmetricKey = data_blocks.get(0);
				byte[] encryptedData =  data_blocks.get(1);
	
				byte[] symmetricKey = decryptKey(decryptKey, encryptedSymmetricKey);
	
				return decrypt(new SecretKeySpec(symmetricKey, ALGO_AES), encryptedData);
			}catch(Exception e) {
				throw new SecurityException(e);				
			}
		}else {
			throw new SecurityException("Received Member can not be null.");
		}
	}

	private byte[] sign(PrivateKey signingKey, byte[] encrypted_block) throws NoSuchAlgorithmException, NoSuchProviderException, InvalidKeyException, SignatureException {
		Signature dsa = Signature.getInstance(ALGO_SHA); 
		dsa.initSign(signingKey);
		dsa.update(encrypted_block);
		return dsa.sign();
	}

	private boolean verify(PublicKey signingVerifyKey,byte[] sigToVerify, byte[] cypherData) throws NoSuchAlgorithmException, NoSuchProviderException, InvalidKeyException, SignatureException {
		Signature sig = Signature.getInstance(ALGO_SHA);
		sig.initVerify(signingVerifyKey);
		sig.update(cypherData, 0, cypherData.length);
	
		return sig.verify(sigToVerify);
	}

	private byte[] encryptKey(PublicKey key, SecretKey skey) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException
	{
		Cipher encryptionRSACipher = Cipher.getInstance(CIPHER_RSA);
		encryptionRSACipher.init(Cipher.ENCRYPT_MODE, key);  
		return encryptionRSACipher.doFinal(skey.getEncoded());
	}

	private byte[] decryptKey(PrivateKey key, byte[] encryptedSymmetricKey) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException
	{
		Cipher	decryptionRSACipher = Cipher.getInstance(CIPHER_RSA);
		decryptionRSACipher.init(Cipher.DECRYPT_MODE, key);  
		return decryptionRSACipher.doFinal(encryptedSymmetricKey);
	}

	private byte[] encrypt(byte[] data, SecretKey skey) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException, InvalidAlgorithmParameterException {
	
		Cipher encryptionAESCipher = Cipher.getInstance(CIPHER_AES);
		encryptionAESCipher.init(Cipher.ENCRYPT_MODE, skey, IV);
		return encryptionAESCipher.doFinal(data);
	
	}

	private byte[] decrypt(SecretKey skey, byte[] data) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException, InvalidAlgorithmParameterException {
		Cipher	decryptionAESCipher = Cipher.getInstance(CIPHER_AES);
		decryptionAESCipher.init(Cipher.DECRYPT_MODE, skey, IV);
		return decryptionAESCipher.doFinal(data);
	
	}

	private static byte[] createBlock(byte[] header, byte[] data) {
		byte[] block = new byte[4+header.length+4+data.length];

		ByteBuffer b = ByteBuffer.allocate(4);
		b.putInt(header.length);
		System.arraycopy(b.array(), 0, block, 0, 4);
		System.arraycopy(header, 0, block, 4, header.length);

		b = ByteBuffer.allocate(4);
		b.putInt(data.length);
		System.arraycopy(b.array(), 0, block, 4+header.length, 4);
		System.arraycopy(data, 0, block, 4+header.length+4, data.length);

		return block;

	}

	private static List<byte[]> splitBlock(byte[] block) {

		int headerLength = ByteBuffer.wrap(Arrays.copyOfRange(block, 0,4)).getInt();
		byte[] header = Arrays.copyOfRange(block, 4, 4+headerLength);


		int dataLength = ByteBuffer.wrap(Arrays.copyOfRange(block, 4+headerLength,4+headerLength+4)).getInt();
		byte[] data = Arrays.copyOfRange(block, 4+headerLength+4, 4+headerLength+4+dataLength);

		List<byte[]> ret = new ArrayList<byte[]>();

		ret.add(header);
		ret.add(data);

		return ret;

	}

}
