package bitmap;

import java.io.*;
import java.lang.*;

import btree.ConstructPageException;
import btree.NodeType;
import global.*;
import diskmgr.*;
import heap.*;

public class BitMapPage extends HFPage{
	
	int keyType;
	
	public BitMapPage() 
	{
		// TODO Auto-generated constructor stub
	}
	
	public BitMapPage(Page page, int keyType) 
	throws IOException,ConstructPageException
	{
		super(page);
	    this.keyType=keyType;   
	}
	
	public byte[] getBMpageArray()
	{
		return this.getHFpageArray();
	}
	
	public void getBMpageArray(byte [] array)
	{
		this.data=array;
	}
	
	  


}
