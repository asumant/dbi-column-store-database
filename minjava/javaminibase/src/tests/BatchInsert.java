package tests;

import global.*;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import btree.BT;
import btree.BTreeFile;
import columnar.*;
import java.io.*;
import java.util.Arrays;
import java.util.StringTokenizer;

import diskmgr.pcounter;

public class BatchInsert {
	
	public static void main (String argv[])
	{	
		String[] colnames = new String[Integer.parseInt(argv[3])];
		AttrType[] type = new AttrType[Integer.parseInt(argv[3])];
		int startread, startwrite; /* keeps track of reads and writes before batchinsert starts */
		
		String filepath = "/home/gaurav/workspace/DBIMinibase/src/tests/";
		System.out.println("1: "+argv[0]);
		System.out.println("2: "+argv[1]);
		System.out.println("3: "+argv[2]);
		System.out.println("4: "+argv[3]);
		SystemDefs sysdef = new SystemDefs(argv[1],100000,100,"Clock");
		
		try {
		FileInputStream fin = new FileInputStream(filepath+argv[0]);
		DataInputStream din = new DataInputStream(fin);
		BufferedReader bin = new BufferedReader(new InputStreamReader(din));
		
		startread = pcounter.rcounter;
		startwrite = pcounter.wcounter;
		
		/* code that reads first line, reads schema and creates Columnarfile */
		String line = bin.readLine();
	
		StringTokenizer st = new StringTokenizer(line);
		int i = 0, tuplelength = 0;
		while(st.hasMoreTokens())	
		{	
			String argToken = st.nextToken();
			StringTokenizer temp = new StringTokenizer(argToken);
			
			String tokenParts1 = temp.nextToken(":");
			String tokenParts2 = temp.nextToken(":");
			
			colnames[i] = new String(tokenParts1);
			
			if (tokenParts2.equals("int"))
			{
				type[i] = new AttrType(AttrType.attrInteger);
				tuplelength = tuplelength + 4;
			}
			else {
				type[i] = new AttrType(AttrType.attrString);
				
				StringTokenizer temp1 = new StringTokenizer(tokenParts2);
				
				temp1.nextToken("(");
				String dummy = temp1.nextToken("(");
				temp1 = null; 
				temp1 =	new StringTokenizer(dummy);
				String dummy1 = temp1.nextToken(")"); 
				Size.STRINGSIZE = Integer.parseInt(dummy1);
				System.out.println("size: "+ Size.STRINGSIZE);
				tuplelength = tuplelength + Size.STRINGSIZE;
			}
			i++;		
		}
		
		/* once you get all data you need, create a columnar file */
		Columnarfile cf = new Columnarfile (argv[2],Integer.parseInt(argv[3]),type);

		cf.setColumnNames(colnames);
		cf.setColumnarFileInfo(Size.STRINGSIZE);
		System.out.println("start inserting tuples ..");
		/* start parsing and inserting records */
		
		byte [] tupledata = new byte[tuplelength];
		int offset = 0, rec =1;
		while((line = bin.readLine()) != null)
		{
			StringTokenizer columnvalue = new StringTokenizer (line);
			System.out.println("Record no.: "+rec++);
			for(AttrType attr: type)
			{
				String column = columnvalue.nextToken();
				if(attr.attrType == AttrType.attrInteger)
				{
					Convert.setIntValue(Integer.parseInt(column), offset, tupledata);
					offset = offset + 4;
				}
				else if (attr.attrType == AttrType.attrString)
				{
					//System.out.println("offset: "+offset);
					Convert.setStrValue(column, offset, tupledata);
					offset = offset + Size.STRINGSIZE;
				}
			}
			cf.insertTuple(tupledata);
			offset = 0;
		
			Arrays.fill(tupledata, (byte)0);
		}
		System.out.println("************Done with tuple insertion!***********");
		System.out.println("Disk reads: "+(pcounter.rcounter-startread)+" Disk writes: "+(pcounter.wcounter-startwrite));
		
		System.out.println("Scan and print tuples...");
		
		TupleScan tscan = new TupleScan(cf);
		TID tid = new TID();
		tid.recordIDs = new RID[Integer.parseInt(argv[3])];
		Tuple record = new Tuple();
		short[] fldOff = {0,(short)Size.STRINGSIZE,(short)(2*Size.STRINGSIZE),(short)(2*Size.STRINGSIZE+4)};
		record.setTupleMetaData(tuplelength, (short)Integer.parseInt(argv[3]), fldOff);
		for (i=0;i<Integer.parseInt(argv[3]);i++)
			tid.recordIDs[i] = new RID();
		i = 0;
		while((record = tscan.getNext(tid)) != null)
		{
			System.out.print("Record:"+i++);
			record.print(type);
		}
		tscan.closetuplescan();
		System.out.println("Completed insert and read operations!!");
		System.out.println("Disk reads: "+(pcounter.rcounter-startread)+" Disk writes: "+(pcounter.wcounter-startwrite));
		SystemDefs.JavabaseBM.resetAllPinCount();
		SystemDefs.JavabaseBM.flushAllPages();
		SystemDefs.JavabaseDB.closeDB();
		} catch (Exception e){
			e.printStackTrace();
		}
	}
}