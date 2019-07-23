package org.tuhin.cluster.security;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.tuhin.cluster.ClusterMember;
import org.tuhin.cluster.ClusterService;

public class SecureMessageObject implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private ClusterMember sentFrom;
	private byte[] cypherBuffer;
	

	public SecureMessageObject(ClusterMember sendingFrom, ClusterMember sendingTo, Object object) throws IOException {
		try {
			sentFrom = sendingFrom;
			cypherBuffer = SecurityProtocol.secure(sendingTo, bundleData(object));
		} catch (SecurityException e) {
			throw new IOException(e);
		}
	}


	
    public ClusterMember getSentFrom() {
		return sentFrom;
	}



	private byte[] bundleData(Object object) throws IOException {
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
		  out = new ObjectOutputStream(bos);   
		  out.writeObject(object);
		  out.flush();
		  return bos.toByteArray();
		} finally {
		  try {
		    bos.close();
		  } catch (IOException ex) {
		  }
		}
	}


	private Object unbundleData(byte[] data) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bis = new ByteArrayInputStream(data);
		ObjectInput in = null;
		try {
		  in = new ObjectInputStream(bis);
		  return in.readObject(); 
		} finally {
		  try {
		    if (in != null) {
		      in.close();
		    }
		  } catch (IOException ex) {
		  }
		}
	}




	public Object getPlainObject(ClusterService service) throws IOException {
		try {
			return unbundleData(SecurityProtocol.unsecure(sentFrom, cypherBuffer));
		} catch (SecurityException | ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	

}
