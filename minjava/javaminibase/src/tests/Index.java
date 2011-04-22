/**
 * 
 */
package tests;

import java.io.File;
import java.io.IOException;

import bitmap.*;
import btree.BT;
import btree.BTreeFile;
import btree.StringKey;

import columnar.ColumnarFileInfo;
import columnar.Columnarfile;
import diskmgr.pcounter;
import global.AttrType;
import global.Convert;
import global.GlobalConst;
import global.RID;
import global.Size;
import global.SystemDefs;
import global.TID;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import bitmap.InstantiateColumnarFile;

/**
 * @author gaurav
 *
 */
public class Index implements GlobalConst{

	private static BitMapFile bmf = null;	
	private static BTreeFile btf=null; 
	private static String columnDbName;
	private static String columnarFileName;
	private static String columnName;
	private static String indexType;
	private static int columnIndex=0;
	private static KeyClass value;
	private static String columnValue; 
	private static int startRead = 0, startWrite = 0;
	private static Columnarfile f = null;
	private static String []colNames;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		AttrType[] types = new AttrType[4];

		types[0] = new AttrType(AttrType.attrInteger);
		types[1] = new AttrType(AttrType.attrString);
		types[2] = new AttrType(AttrType.attrString);
		types[3] = new AttrType(AttrType.attrInteger);
		columnDbName=args[1];
		columnarFileName=args[2];
		columnName=args[3];
		indexType=args[4];
		columnValue=args[5];
		startRead = pcounter.rcounter;
		startWrite = pcounter.wcounter;

		SystemDefs sysdef = new SystemDefs(columnDbName,10000,100,"Clock");
		f=new Columnarfile(columnarFileName, 4, types);
		ColumnarFileInfo cInfo = f.getColumnarFileInfo(columnarFileName+".hdr");
		colNames = cInfo.getColumnNames();
		f.setColumnNames(colNames);
		
		for(String str: colNames)
		{
			if(str.equalsIgnoreCase(columnName))
				break;
			columnIndex++;
		}
		
		try  
	    {  
	       Integer.parseInt( columnValue );  
	       value=InstantiateColumnarFile.setKeyClass(value, 0, columnValue);
	    }  
	    catch( Exception e)  
	    {  
	    	value=InstantiateColumnarFile.setKeyClass(value, 1, columnValue);
	    } 

		
		System.out.println ("\n  Begin Index Test: \n");
		final boolean OK = true;
		final boolean FAIL = false;
		int choice=100;
		final int reclen = 128;

		boolean status = OK;
		TID tid = new TID();

			//replace by method to get the index of the column from the columnar file 
			Heapfile reqHFile=f.getHeapfileForColumname(columnName);
			
			
			if(indexType.equalsIgnoreCase("Btree"))
			{
				try 
				{
					RID rid=new RID();
					Scan hScan=reqHFile.openScan();
					Tuple hTuple=null;
					System.out.println("btree name"+"btree"+columnName);
					try{
					SystemDefs.JavabaseDB.delete_file_entry("btree"+columnName);
					}catch(Exception e)
					{
					}
					
					btf = new BTreeFile("btree"+columnName, AttrType.attrInteger, INTSIZE, 1/*delete*/);

					while((hTuple=hScan.getNext(rid))!=null)
					{
						int temp=Convert.getIntValue(0, hTuple.getData());
						btf.insert(new btree.IntegerKey(temp), rid);
					}
					BT.printBTree(btf.getHeaderPage());
					System.out.println("printing leaf pages...");
					BT.printAllLeafPages(btf.getHeaderPage());
					
					System.out.println("created Btree index...");
				} catch (Exception e) 
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			else if(indexType.equalsIgnoreCase("Bitmap"))
			{
				try 
				{
					String bitName="bitmap."+columnName+columnValue;
					BitMapFile bm = new BitMapFile(bitName);
					bm.deleteFile();
					bmf=InstantiateColumnarFile.getColumnarFile(bmf, f, value, columnIndex,bitName, reqHFile);				
					bmf.printBitMapFile();
					System.out.println("created Bitmap index...");
				} catch (Exception e) 
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			System.out.println("end of test....");
			System.out.println(pcounter.rcounter+" and "+pcounter.wcounter);
			try {
				SystemDefs.JavabaseBM.resetAllPinCount();
				SystemDefs.JavabaseBM.flushAllPages();
				SystemDefs.JavabaseDB.closeDB();
				} catch (Exception e) {
					e.printStackTrace();
				}
		}
	
}
