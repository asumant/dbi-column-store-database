package bitmap;

import global.SystemDefs;
import heap.Heapfile;
import bitmap.IntegerKey;
import columnar.Columnarfile;

public class InstantiateColumnarFile {

	public static BitMapFile getColumnarFile(BitMapFile bmf, Columnarfile f, KeyClass key, int colNo, String name, Heapfile reqFile)
	{
		try
		{
			bmf=new BitMapFile(name);
			System.out.println("in try......");
			if(SystemDefs.newFile == true)
			{
				System.out.println("created a new bm file");
				if(key instanceof IntegerKey)
					bmf = new BitMapFile(name, f, colNo, (IntegerKey) key, 1);
				else if(key instanceof StringKey)
					bmf = new BitMapFile(name, f, colNo, (StringKey) key, 1);
				//System.out.println("in catch : try......");
				SystemDefs.newFile = false;
				bmf.createBitMap(reqFile);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return bmf;
	}

	public static KeyClass setKeyClass(KeyClass k, int choice, String columnValue)
	{
		final int i;
		try
		{
			i=Integer.parseInt(columnValue);
			if(choice==0)
				k= (IntegerKey) new IntegerKey(i);
		}
		catch(Exception e)
		{
			if(choice==1)
				k= (StringKey) new StringKey(columnValue);			
		}
		return k;
	}

}

