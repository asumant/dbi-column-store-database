package bitmap;

import java.util.Arrays;

import global.Convert;

public class testByte {

	public static void main(String args[])
	{
		byte[] t1=new byte[4];
		byte[] t2=new byte[60];
		try
		{
		Convert.setIntValue(1, 0, t1);
		Convert.setIntValue(1, 0, t2);
		if(Arrays.equals(t1, t2))
			System.out.println("true");
		else
			System.out.println("false");
		}
		catch(Exception e)
		{e.printStackTrace();}
	}
}
