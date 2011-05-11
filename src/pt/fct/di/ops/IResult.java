package pt.fct.di.ops;

import java.io.IOException;
//import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public interface IResult {

	public void setOpSeq(int seq);
	public void setVersionVector(long[] vector);
	
	public int getOpSeq();
	public int getCode();
	public long[] getVersionVector();
	
	public byte[] serialize() throws IOException;
	public void serialize(ObjectOutputStream oos) throws IOException, OpException;
	public byte[] toByteArray() throws OpException;
//	public void deserialize(byte[] buffer) throws IOException, ClassNotFoundException;
//	public void deserialize(ObjectInputStream ois) throws IOException, ClassNotFoundException;
	public boolean opValidation() throws OpException;
	
}
