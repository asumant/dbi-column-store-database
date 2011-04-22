package tests;

import global.AttrType;
import global.Convert;
import global.PageId;
import global.RID;
import global.Size;
import global.SystemDefs;
import global.TID;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import bitmap.BitMapFile;
import bitmap.InstantiateColumnarFile;
import bitmap.KeyClass;
import btree.BT;
import btree.BTFileScan;
import btree.BTreeFile;
import btree.IntegerKey;
import btree.KeyDataEntry;
import btree.StringKey;
import columnar.ColumnarFileInfo;
import columnar.Columnarfile;
import columnar.TupleScan;
import diskmgr.pcounter;

public class Delete_Query {
	
	private static BitMapFile bmf = null;	
	private static BTreeFile btf=null; 
	private static String columnDbName;
	private static String columnarFileName;
	private static ArrayList <String> columnNames=new ArrayList<String>();
	private static String accessType;
	private static String valueConstraint;
	private static String numBuf;
	private static String columnName;
	private static String operator;
	private static String columnValue;
	private static KeyClass value=null;
	private static int columnIndex=0;
	private static int columnAbsoluteIndex=0;
	private static Columnarfile f = null;
	private static AttrType[] types;
	private static int startRead = 0, startWrite = 0;
	private static boolean isPurge = false;
	private static String []colNames;
	
	/**
	 * @param args
	 */
	
	public boolean isInteger( String input )  
	 {  
	    try  
	    {  
	       Integer.parseInt( input );  
	       return true;  
	    }  
	    catch( Exception e)  
	    {  
	       return false;  
	    }  
	} 
	
	public static void parseValue(String tempValue)
	{
		StringTokenizer st = new StringTokenizer(tempValue, ":");
		System.out.println("0..."+tempValue);
		columnName=st.nextToken();
		System.out.println("1..."+columnName);
		for(String str: colNames)
		{
			if(str.equalsIgnoreCase(columnName))
				break;
			columnIndex++;
		}
		operator=st.nextToken();
		System.out.println("1..."+operator);
		columnValue=st.nextToken();
		System.out.println("1..."+columnValue);
		try  
	    {  
	       Integer.parseInt( columnValue );  
	       value=InstantiateColumnarFile.setKeyClass(value, 0, columnValue);
	    }  
	    catch( Exception e)  
	    {  
	    	value=InstantiateColumnarFile.setKeyClass(value, 1, columnValue);
	    } 
	}
	
	public static void main(String[] args) throws InvalidTupleSizeException, IOException {
		// TODO Auto-generated method stub
		types = new AttrType[4];
		types[0] = new AttrType(AttrType.attrString);
		types[1] = new AttrType(AttrType.attrString);
		types[2] = new AttrType(AttrType.attrInteger);
		types[3] = new AttrType(AttrType.attrInteger);
		startRead = pcounter.rcounter;
		startWrite = pcounter.wcounter;
		columnDbName=args[1];
		columnarFileName=args[2];

		accessType=args[args.length-2];
		numBuf=args[args.length-3];
		valueConstraint=args[args.length-4];
		for(int i=3;i<args.length-4;i++)
			columnNames.add(args[i]);

		
		SystemDefs sysdef = new SystemDefs(columnDbName,10000,Integer.parseInt(numBuf),"Clock");
		ColumnarFileInfo cInfo = new  ColumnarFileInfo(columnarFileName+".hdr");
		f=new Columnarfile(columnarFileName, 4, types);
		cInfo = f.getColumnarFileInfo(columnarFileName+".hdr");
		colNames = cInfo.getColumnNames();
		f.setColumnNames(colNames);

		Delete_Query.parseValue(valueConstraint);
		isPurge = Boolean.parseBoolean(args[args.length -1]);

		System.out.println ("\n  Begin Index Test: \n");
		TID tid = new TID();
		for(String str: colNames)
		{
			if(str.equalsIgnoreCase(columnName))
				break;
			columnAbsoluteIndex++;
		}
		
/*		final boolean OK = true;
		final boolean FAIL = false;
		int choice=50;
		final int reclen = 128;

		boolean status = OK;
		

		types = new AttrType[4];

		types[0] = new AttrType(AttrType.attrInteger);
		types[1] = new AttrType(AttrType.attrString);
		types[2] = new AttrType(AttrType.attrString);
		types[3] = new AttrType(AttrType.attrInteger);

		System.out.println ("  - Create a columnar file\n");
		try {
			
			for(String str: colNames)
			{
				if(str.equalsIgnoreCase(columnName))
					break;
				columnAbsoluteIndex++;
			}
			f = new Columnarfile(columnarFileName, 4, types);
			f.setColumnNames(colNames);
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println ("*** Could not create columnar file\n");
			e.printStackTrace();
		}

		if ( status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
				!= SystemDefs.JavabaseBM.getNumBuffers() ) {
			System.err.println ("*** The heap file has left pages pinned\n");
			status = FAIL;
		}

		if ( status == OK ) {
			System.out.println ("  - Add " + choice + " records to the file\n");
			for (int i =0; (i < choice) && (status == OK); i++) {

				//fixed length record
				dummyrec3 rec = new dummyrec3(reclen);
				rec.ival1 = i;
				rec.ival2 = i;
				rec.test = "tuple" + i;
				rec.name = "record" + i;

				try {
					tid = f.insertTuple(rec.toByteArray());
				}
				catch (Exception e) {
					status = FAIL;
					System.err.println ("*** Error inserting record " + i + "\n");
					e.printStackTrace();
				}

				if ( status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
						!= SystemDefs.JavabaseBM.getNumBuffers() ) {

					System.err.println ("*** Insertion left a page pinned\n");
					status = FAIL;
				}
			}

			try {
				if ( f.getTupleCnt() != choice ) {
					status = FAIL;
					System.err.println ("*** File reports " + f.getTupleCnt() + 
							" records, not " + choice + "\n");
				}
			}
			catch (Exception e) {
				status = FAIL;
				System.out.println (""+e);
				e.printStackTrace();
			}
		}
*/		
		
		Heapfile reqHFile=f.getHeapfileForColumname(columnName);
		
		int keyType=0, lowkeyInt=0, hikeyInt=0;
		IntegerKey lowkey=new IntegerKey(0);
		IntegerKey hikey=new IntegerKey(0);
		BTFileScan scan=null;
		KeyDataEntry entry;
		RID rid=new RID();
		
		Scan hScan;
		try {
			hScan = reqHFile.openScan();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			hScan=null;
		}
		
		if(accessType.equalsIgnoreCase("Btree"))
		{
			try 
			{
				Tuple hTuple=null;
				if(value instanceof bitmap.IntegerKey)
					btf = new BTreeFile("btree"+columnName, AttrType.attrInteger, 4, 1/*delete*/);
				
				BT.printBTree(btf.getHeaderPage());
				System.out.println("printing leaf pages...");
				//BT.printAllLeafPages(btf.getHeaderPage());				
				System.out.println("created Btree index...");
				  
				keyType=AttrType.attrInteger;
				System.out.println("Please input the LOWER integer key (null if -3): ");
				  lowkeyInt=GetStuff.getChoice(); 
				  lowkey=new IntegerKey(lowkeyInt);
				  System.out.println("Please input the HIGHER integer key (null if -2): ");
				  hikeyInt=GetStuff.getChoice(); 
				  hikey=new IntegerKey(hikeyInt);
				  if( lowkeyInt==-3)     
				    lowkey=null;
				  if( hikeyInt==-2)     
				    hikey=null;
				  if( hikeyInt!=-1 || lowkeyInt!=-1 ) 
				    scan= btf.new_scan(lowkey, hikey);
				  int position=0;
				  while((entry=scan.get_next())!=null)
				  {
					  if(entry!=null)
					  {
						  char sw=operator.charAt(0);
						  boolean eval=false;
						  switch(sw)
						  {
						  case '>':
							  if(entry.key instanceof IntegerKey)
							  {
								  if(((IntegerKey)entry.key).getKey()>((bitmap.IntegerKey)value).getKey())
									  eval=true;
								  else
									  eval=false;
							  }
							  else if(entry.key instanceof StringKey)
							  {
								  if(((StringKey)entry.key).getKey().compareTo(((bitmap.StringKey)value).getKey())>0)
									  eval=true;
								  else
									  eval=false;
							  }
							  break;
						  case '<':
							  if(entry.key instanceof IntegerKey)
							  {
								  if(((IntegerKey)entry.key).getKey()<((bitmap.IntegerKey)value).getKey())
									  eval=true;
								  else
									  eval=false;
							  }
							  else if(entry.key instanceof StringKey)
							  {
								  if(((StringKey)entry.key).getKey().compareTo(((bitmap.StringKey)value).getKey())>0)
									  eval=true;
								  else
									  eval=false;
							  }
							  break;
						  case '!':
							  if(entry.key instanceof IntegerKey)
							  {
								  if(((IntegerKey)entry.key).getKey()>((bitmap.IntegerKey)value).getKey())
									  eval=true;
								  else
									  eval=false;
							  }
							  else if(entry.key instanceof StringKey)
							  {
								  if(((StringKey)entry.key).getKey().compareTo(((bitmap.StringKey)value).getKey())!=0)
									  eval=true;
								  else
									  eval=false;
							  }
							  break;
						  case '=':
							  if(entry.key instanceof IntegerKey)
							  {
								  if(((btree.IntegerKey)entry.key).getKey().equals(((bitmap.IntegerKey)value).getKey()))
									  eval=true;
								  else
									  eval=false;
							  }
							  else if(entry.key instanceof StringKey)
							  {
								  if(((StringKey)entry.key).getKey().compareTo(((bitmap.StringKey)value).getKey())==0)
									  eval=true;
								  else
									  eval=false;
							  }
							  break;

						  }
						  
						  if(eval)
						  {
							  System.out.println("Deleting from Btree: "+ entry.key + " " + entry.data);
							  RID tempRid=new RID();
							  System.out.println("SCAN RESULT: "+ entry.key + " " + entry.data);
							  tempRid=((btree.LeafData)entry.data).getData();
							  //btf.Delete(new IntegerKey(((IntegerKey)entry.key).getKey()), tempRid);
							  int pos=reqHFile.RidToPos(tempRid);
							  f.markTupleDeleted(getProjection(pos));
							  eval=false;
						  }
							  
					  }
					  position++;
				  }
				  System.out.println("AT THE END OF SCAN!");
				  System.out.println("Disk Reads"+ (pcounter.rcounter - startRead));
				  System.out.println("Disk Writes"+ (pcounter.wcounter - startWrite));
			} catch (Exception e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		
		if(accessType.equalsIgnoreCase("Bitmap"))
		{
			try 
			{
				String bitName="bitmap."+columnName+columnValue;
				bmf=InstantiateColumnarFile.getColumnarFile(bmf, f, value, columnIndex, bitName,reqHFile);				
				bmf.printBitMapFile();
				System.out.println("created Bitmap index...");
				bmf.deleteTuplesBitMap(reqHFile, f, types);
				System.out.println("Disk Reads"+ (pcounter.rcounter - startRead));
				System.out.println("Disk Writes"+ (pcounter.wcounter - startWrite));
			} catch (Exception e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		

		if(accessType.equalsIgnoreCase("Columnscan"))
		{
			int position = 0;
			ArrayList<TID> tidList = new ArrayList<TID>();
			
			try 
			{
				Tuple t = new Tuple();
				
				boolean deleteFlag = false;
				if (value instanceof bitmap.IntegerKey)
				{
					while ((t = hScan.getNext(rid))!=null)
					{
						position++;
						 char sw=operator.charAt(0);
						  boolean eval=false;
						  switch(sw)
						  {
						  case '>':
							  if (((bitmap.IntegerKey)value).getKey().compareTo((Convert.getIntValue(0,t.getData())))<0)
								{
								  tidList.add(getProjection(position));									
								}
							  
							  break;
						  case '<':
							  if (((bitmap.IntegerKey)value).getKey().compareTo((Convert.getIntValue(0,t.getData())))>0)
								{
								  tidList.add(getProjection(position));
								}
							  
							  break;
						  case '!':
							  
							  if (!((bitmap.IntegerKey)value).getKey().equals(Convert.getIntValue(0,t.getData())))
								{
								  tidList.add(getProjection(position));	
								}
							 
							  break;
						  case '=':

								if (((bitmap.IntegerKey)value).getKey().equals(Convert.getIntValue(0,t.getData())))
								{
									tidList.add(getProjection(position));								
								}
							  break;

						  }	  
					}
					
				}
				else if (value instanceof bitmap.StringKey)
				{
					while ((t = hScan.getNext(rid))!=null)
					{
						  position++;
						  char sw=operator.charAt(0);
						  boolean eval=false;
						  switch(sw)
						  {
						  case '>':
							  if (((bitmap.StringKey)value).getKey().compareTo((Convert.getStrValue(0,t.getData(),Size.STRINGSIZE)))<0)
								{
								  tidList.add(getProjection(position));
								}
							  
							  break;
						  case '<':
							  if (((bitmap.StringKey)value).getKey().compareTo((Convert.getStrValue(0,t.getData(),Size.STRINGSIZE)))>0)
								{
								  tidList.add(getProjection(position));
								  												
								}
							  
							  break;
						  case '!':
							  
							  if (!(((bitmap.StringKey)value).getKey().compareTo((Convert.getStrValue(0,t.getData(),Size.STRINGSIZE)))==0))
								{
								  tidList.add(getProjection(position));
								}
							 
							  break;
						  case '=':

							  if (((bitmap.StringKey)value).getKey().compareTo((Convert.getStrValue(0,t.getData(),Size.STRINGSIZE)))==0)
								{
								  tidList.add(getProjection(position));
								  									
								}
							  break;

						  }
						  
					}
										
				}
				hScan.closescan();
				
				int i = 0 ;
				for (TID tid1 : tidList)
				{
					f.markTupleDeleted(tid1);
				}
				
				System.out.println("Disk Reads"+ (pcounter.rcounter - startRead));
				System.out.println("Disk Writes"+ (pcounter.wcounter - startWrite));
				
			} 
			catch (Exception e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}	
		if(accessType.equalsIgnoreCase("Filescan"))
		{
			try
			{
				TupleScan ts = new TupleScan(f);
				tid.recordIDs = new RID[f.getnumColumns()];
				for(int j =0 ; j < f.getnumColumns() ; j++)
					tid.recordIDs[j] = new RID();
				
				Tuple t = null;
				while ((t=ts.getNext(tid))!=null)
				{
					if(value instanceof bitmap.IntegerKey)
					{
						System.out.println(t.getIntFld(columnAbsoluteIndex+1));
						char sw=operator.charAt(0);
						  switch(sw)
						  {
						  case '>':
							  if (((bitmap.IntegerKey)value).getKey().compareTo(t.getIntFld(columnAbsoluteIndex+1))<0)
								{
								  f.markTupleDeleted(tid); 
								}
							  
							  break;
						  case '<':
							  if (((bitmap.IntegerKey)value).getKey().compareTo(t.getIntFld(columnAbsoluteIndex+1))>0)
								{
								  f.markTupleDeleted(tid);							
								}
							  
							  break;
						  case '!':
							  
							  if (!(((bitmap.IntegerKey)value).getKey().compareTo(t.getIntFld(columnAbsoluteIndex+1))==0))
								{
								  f.markTupleDeleted(tid);								
								}
							 
							  break;
						  case '=':

							  if (((bitmap.IntegerKey)value).getKey().compareTo(t.getIntFld(columnAbsoluteIndex+1))==0)
								{
								  f.markTupleDeleted(tid);
								}
							  break;

						  }
						  						
					}
					else if(value instanceof bitmap.StringKey)
					{
						char sw=operator.charAt(0);
						  switch(sw)
						  {
						  case '>':
							  
							  if (((bitmap.StringKey)value).getKey().compareTo(t.getStrFld(columnAbsoluteIndex+1))<0)
								{
								  f.markTupleDeleted(tid);
								  
								}
							  
							  break;
						  case '<':
							  if (((bitmap.StringKey)value).getKey().compareTo(t.getStrFld(columnAbsoluteIndex+1))>0)
								{
								  f.markTupleDeleted(tid);							
								}
							  
							  break;
						  case '!':
							  
							  if (!(((bitmap.StringKey)value).getKey().compareTo(t.getStrFld(columnAbsoluteIndex+1))==0))
								{
								  f.markTupleDeleted(tid);							
								}
							 
							  break;
						  case '=':

							  if (((bitmap.StringKey)value).getKey().compareTo(t.getStrFld(columnAbsoluteIndex+1))==0)
								{
								  f.markTupleDeleted(tid);
								}
							  break;

						  }
						  										
					}
				}
				System.out.println("Disk Reads"+ (pcounter.rcounter-startRead));
				System.out.println("Disk Writes"+ (pcounter.wcounter-startWrite));
				
			}
			catch (Exception e)
			{
				
			}
		}
		
		f.showDeleteDump();
		if (isPurge)
		{
			  	System.out.println("Purging all the deleted records");
				f.purgeAllDeletedTuples();
				f.showDeleteDump();
		}
		try {
			SystemDefs.JavabaseBM.resetAllPinCount();
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseDB.closeDB();
		} catch (Exception e)	{
			e.printStackTrace();
		}
		
	}
	
	public static TID deleteProjection(int position) throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception
	{
		int colSize = colNames.length;
		ArrayList<Tuple> arrTuples=new ArrayList<Tuple>();
		RID rid = new RID();
		Heapfile hf;
		Tuple t;
		TID tid=new TID(colSize);
		tid.recordIDs=new RID[colSize];
		tid.pos=position;
		tid.numRIDs=colSize;
		System.out.print("[");
		for(int j=0;j<colSize;j++)
			tid.recordIDs[j]=new RID();
		
		for (int i = 0 ; i < colSize ; i++)
		{
			hf = f.getHeapfileForColumname(colNames[i]);
			rid = hf.PosToRid(position);
			t = hf.getRecord(rid);
			tid.recordIDs[i]=rid;
			System.out.print("RID of current: "+rid.pageNo+"->"+rid.slotNo);
			if(t.getLength()>4)
				System.out.print("deleting: "+Convert.getStrValue(0, t.getData(), t.getLength())+", ");
			else
				System.out.print("deleting: "+Convert.getIntValue(0, t.getData())+", ");
			arrTuples.add(hf.getRecord(rid));
		}
		System.out.println("]");
		return tid;
	}

	
	public static TID getProjection(int position) throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception
	{
		int colSize = colNames.length;
		ArrayList<Tuple> arrTuples=new ArrayList<Tuple>();
		RID rid = new RID();
		Heapfile hf;
		Tuple t;
		TID tid=new TID();
		tid.recordIDs=new RID[colSize];
		tid.pos=position;
		tid.numRIDs=colSize;
		for(int j=0;j<colSize;j++)
			tid.recordIDs[j]=new RID();

		System.out.print("[");
		for (int i = 0 ; i < colSize ; i++)
		{
			hf = f.getHeapfileForColumname(colNames[i]);
			rid = hf.PosToRid(position);
			t = hf.getRecord(rid);
			tid.recordIDs[i]=rid;
			if(t.getLength()>4)
				System.out.print(Convert.getStrValue(0, t.getData(), t.getLength())+", ");
			else
				System.out.print(Convert.getIntValue(0, t.getData())+", ");
			arrTuples.add(hf.getRecord(rid));
		}
		System.out.println("]");
		return tid;
	}
}
	