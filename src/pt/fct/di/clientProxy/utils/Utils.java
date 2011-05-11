package pt.fct.di.clientProxy.utils;

import java.util.Random;

public class Utils {

	public static long[] mergeArrays(long[] arr1, long[] arr2)
	{
		if(arr1.length != arr2.length)
			return new long[0];
		else
		{
			long[] result = new long[arr1.length];
			for(int i=0; i<arr1.length; i++)
				result[i] = Math.max(arr1[i], arr2[i]);
			return result;
		}
	}
	
	public static final int makeIntFromByte4(byte[] b) {
		return b[0]<<24 | (b[1]&0xff)<<16 | (b[2]&0xff)<<8 | (b[3]&0xff);
	}
	public static final byte[] makeByte4FromInt(int i) {
		return new byte[] { (byte)(i>>24), (byte)(i>>16), (byte)(i>>8), (byte)i };
	}
	
	public static void main(String[] args)
	{
		Random rm = new Random(Long.MAX_VALUE);
		long[] a1 = new long[3];
		a1[0] = rm.nextLong();
		a1[1] = rm.nextLong();
		a1[2] = rm.nextLong();
		
		long[] a2 = new long[3];
		a2[0] = rm.nextLong();
		a2[1] = rm.nextLong();
		a2[2] = rm.nextLong();
		
		long[] result = Utils.mergeArrays(a1, a2);
		for(int i=0; i<3; i++)
		{
			System.out.println(a1[i]+" "+a2[i]+" = "+result[i]);
		}
	}
}