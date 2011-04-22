package columnar;

import java.io.*;
import java.util.ArrayList;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import btree.*;

/*  This Columnarfile implementation contains multiple heapfiles 
 *  one per attribute in a relational Table. We maintain a
 *  array of HeapFile objects corresponding to all these files also 
 *  the field position (rank + slot no) which helps in fetching records 
 *  from different heap files managing different attributes.
 *
 */


/*
* April 9, 1998
*/


public class Columnarfile implements GlobalConst {
   
  private String filename;
  private Heapfile columnarFile;
  private int numColumns;
  AttrType[] type;
  public Heapfile[] hfColumns;
  public String [] columnNames;
  public String [] heapFileNames;
  private byte [] deletedTIDsData;
  private int tupleLength;
  private Heapfile deleteDump;
  private Heapfile deletedTIDsHeapFile;
  ArrayList<RID> deletedTIDs = new ArrayList<RID>();
  int deleteCount;
  private ColumnarFileInfo cfInfo;
  
  public int getnumColumns()
  {
	  return numColumns;
  }
  
  public String getFilename() {
	return filename;
  }

  public void setFilename(String filename) {
	this.filename = filename;
  }

  public Heapfile getDeleteDump() {
	return deleteDump;
  }

  public void setDeleteDump(Heapfile deleteDump) {
	this.deleteDump = deleteDump;
  }

  public ArrayList<RID> getDeletedTIDs() {
	return deletedTIDs;
  }

  public void setDeletedTIDs(ArrayList<RID> deletedTIDs) {
	this.deletedTIDs = deletedTIDs;
  }

  public int getDeleteCount() {
	return deleteCount;
  }

  public void setDeleteCount(int deleteCount) {
	this.deleteCount = deleteCount;
  }

  public int getTupleLength()  {
	  
	  return tupleLength;
  }
  public String[] getColumnNames() {
		return columnNames;
  }

  public void setColumnNames(String[] columnNames) {
		this.columnNames = columnNames;
	}
  

  public Heapfile[] gethfColumns()
  {
	  return hfColumns;
  }

  public Heapfile getHeapfileForColumname(String columnName)
  {
	  int i = 0;
	  boolean found = false;
	  for (String columnname : columnNames)
	  {
		  if (columnName.equalsIgnoreCase(columnname))
		  {
			  found = true;
			  break;
		  }
		  i++;
	  }
	  if (found)
	  return hfColumns[i];
	  else return null;
  }
  
  public AttrType  getAttributeTypeForColumname(String columnName)
  {
	  int i = 0;
	  boolean found = false;
	  for (String columnname : columnNames)
	  {
		  if (columnName.equalsIgnoreCase(columnname))
		  {
			  found = true;
			  break;
		  }
		  i++;
	  }
	  if (found)
	  return type[i];
	  else return null;
  }

  
  public Columnarfile(String name, int numColumns, AttrType[] type)
  {
	  filename = name;
	  int i = 0;
	  this.type = type;
	  tupleLength = 0;
	  deleteCount = 0;
	  this.numColumns = numColumns;
	  hfColumns = new Heapfile[numColumns];
	  columnNames = new String [numColumns];
	  heapFileNames = new String [numColumns];
	  
	  // Meta data of Columnar File
	  cfInfo = new ColumnarFileInfo(name, numColumns);
	  
	  try	{
	  // Columnar File Name
	  columnarFile = new Heapfile(name+".hdr");
	  cfInfo.setColumnarFileName(name);
	  deleteDump = new Heapfile(filename+".del");
	  deletedTIDsHeapFile = new Heapfile("deletedTids");
	  for (AttrType attr: type)
	  {
		  heapFileNames[i] = name.concat(Integer.toString(i));
		  hfColumns[i] = new Heapfile(heapFileNames[i]);
		   if(attr.attrType == AttrType.attrInteger)
		  {
			   tupleLength = tupleLength + INTSIZE;
		  }
		  else if (attr.attrType == AttrType.attrString)
		  {
			  tupleLength = tupleLength + Size.STRINGSIZE;
		  }
		  i++;
	  }
	  cfInfo.setTupleLength(tupleLength);
	  }
	  catch (Exception e)
	  {
		  e.printStackTrace();
	  }
  }

  public Columnarfile()
  {
	  
  }
  
  public void setColumnarFileInfo(int stringSize)		{
	    
	  try  {
		  
	  	  //cf.deletedTIDs = columnarfileinfo.getDeletedTIDs();
		  cfInfo.setStringSize(stringSize);
		   for(int i = 0 ; i < numColumns ; i++)	{
			   cfInfo.setHeapFileNames(heapFileNames[i], i);
			   cfInfo.setAttributeType(type[i].attrType, i);
			   cfInfo.setColumnNames(columnNames[i], i);
			}
		   cfInfo.convertToTuple();
		   columnarFile.insertRecord(cfInfo.getData());
	  }
	  catch (Exception e)	{
		  
		  e.printStackTrace();
	  }
	  
	}
  
  public Columnarfile getColumnarFile(ColumnarFileInfo columnarfileinfo)		{
	  
	  Columnarfile cf = null;
	  try	  {
		
		  if (columnarfileinfo != null)		{
			  
			  cf = new Columnarfile();
			  //cf.deletedTIDs = columnarfileinfo.getDeletedTIDs();
			  cf.filename = columnarfileinfo.getColumnarFileName();
			  cf.numColumns = columnarfileinfo.getNumColumns();
			  for(int i = 0 ; i < cf.numColumns ; i++)	{
			  
				  cf.hfColumns[i] = new Heapfile(columnarfileinfo.getHeapFileNames()[i]);
				  cf.type[i] = new AttrType(columnarfileinfo.getAttributeType()[i]);
			  }
		  }
		  else
		  {
			  System.out.println("Class :"+this.getClass()+"Method :"+this.getClass().getEnclosingMethod()+": Columnar File Info Object is Null");
			  return null;
		  }
		  
	  }
	  catch (Exception e)	{
		  
		  e.printStackTrace();
	  }
	  
	  return cf;
	  
  }
  
  public ColumnarFileInfo getColumnarFileInfo (String name)		{
	  if(name != null)	  {
		  ColumnarFileInfo columnarInfo = null;
		  
		  try	  {
			  
			  Heapfile columnarInfoFile = new Heapfile(name);
			  RID rid = new RID();
			  Tuple tuple = new Tuple();
			  Scan s = columnarInfoFile.openScan();
			  tuple = s.getNext(rid);
			  columnarInfo = new ColumnarFileInfo();
			  Size.STRINGSIZE = columnarInfo.getStringSize(tuple);
			  columnarInfo.getColumnarFileInfo(tuple);
			  System.out.println("num of columns"+columnarInfo.getNumColumns());
			    
		  }
		  catch (Exception e)	  {
			  
			  e.printStackTrace();
		  }
		  
		  return columnarInfo;
	  }
	  else	{
		  
		  return null;
	  }
		  
	  
  }
  
  public Tuple getTuple(TID tid)
  {
	  byte[] fullTuple = new byte[tupleLength];
	  int offset = 0;
	  Tuple t = new Tuple();
	  try {
	  for (int i= 0; i < numColumns ; i++)
	  {
		 t = hfColumns[i].getRecord(tid.recordIDs[i]);
		 
		 if (type[i].attrType == AttrType.attrInteger)	{
			  int intAttr = Convert.getIntValue(offset,t.returnTupleByteArray());
			  try {
			  Convert.setIntValue(intAttr,offset,fullTuple);
			  }catch(Exception e){
				  e.printStackTrace();
			  }
			  offset = offset + INTSIZE;
		  }
		  if (type[i].attrType == AttrType.attrString)	{
			  String strAttr = Convert.getStrValue(offset,t.returnTupleByteArray(),Size.STRINGSIZE);
			  Convert.setStrValue(strAttr,offset,fullTuple);
			  offset = offset + Size.STRINGSIZE;
		  }
	  }
	  t.tupleSet(fullTuple,0,fullTuple.length);
	  short[] fldOff = new short[numColumns];
	  int i = 0, off = 0;
	  for(AttrType t1: type)
	  {
		  if (t1.attrType == AttrType.attrInteger)	{
			  fldOff[i++] =(short) off;
			  off = off + INTSIZE;
		  }
		  else if (t1.attrType == AttrType.attrInteger)	{
			  fldOff[i++] =(short) off;
			  off = off + Size.STRINGSIZE;
		  }
	  }
	  t.setTupleMetaData(tupleLength, (short)numColumns, fldOff);
	  }
	  catch (Exception e)
	  {
		  e.printStackTrace();
	  }
	  return t;
  }
  
  public KeyClass getValue(TID tid, int column)
  {
	  KeyClass retValue = null;
	  IntegerKey k= new IntegerKey(0);
	  StringKey str= new StringKey("default");
	  try{
	  byte[] colValue = hfColumns[column].getRecord(tid.recordIDs[column]).returnTupleByteArray();
	  
	  if (type[column].attrType == AttrType.attrInteger)	{
		
		  k.setKey(Convert.getIntValue(0,colValue));
		  retValue = k;
	  }
	  else if (type[column].attrType == AttrType.attrString)	{
		  
		  str.setKey(Convert.getStrValue(0,colValue,Size.STRINGSIZE));
		  retValue = str;
	  }
	  }catch(Exception e)
	  {
		  e.printStackTrace();
	  }
	  return retValue;
  }
  
  public int getTupleCnt()
  {
	 /* As we are maintaining Fixed length record and also for null values slots are wasted,
	  * we can get a tuple count by querying tuple count any one heapfile corresponding to 
	  * any column. For simplicity we have considered first heapfile i.e. hfColumns[0]  
	  */
	  int recCnt = 0;
	  try {
	  recCnt = hfColumns[0].getRecCnt();
	  }catch(Exception e)	{
		  e.printStackTrace();
	  }
	  return recCnt;
  }
  
  
  public TupleScan openTupleScan()
  throws InvalidTupleSizeException, IOException
  {
	  TupleScan newScan = new TupleScan(this);
	  return newScan;
  }
  
  /* Yet need a scan support */
  public Scan openColumnScan(int columnNo)
  {
	  return null;
  }
  
  
  public boolean updateTuple(TID tid, Tuple newtuple) 
  throws InvalidSlotNumberException, 
  InvalidUpdateException, 
  InvalidTupleSizeException,
  HFException, 
  HFDiskMgrException,
  HFBufMgrException,
  Exception
  {
	  int i = 0;
	  
	  if (tupleLength != newtuple.getLength())
		  throw new InvalidUpdateException(null, "Columnarfile: invalid record update");
	  
	  for (;i<numColumns;i++)
	  {
		  if(!updateColumnofTuple(tid,newtuple,i+1))
			  return false;
			  
	  }
	  return true;
  }
  public boolean updateColumnofTuple(TID tid, Tuple newtuple, int column)
  {
	  int intVal;
	  String strVal;
	  Tuple columnTuple = null;
	  try {
	  if (type[column-1].attrType == AttrType.attrInteger)	{
		  intVal = newtuple.getIntFld(column);
		  columnTuple = new Tuple(INTSIZE);
		  short[] fldOff = {0};
		  columnTuple.setTupleMetaData(INTSIZE,(short)1, fldOff);
		  columnTuple.setIntFld(1, intVal);
	  }
	  else if (type[column-1].attrType == AttrType.attrString)	{
		  strVal = newtuple.getStrFld(column);
		  columnTuple = new Tuple(Size.STRINGSIZE);
		  short[] fldOff = {0};
		  columnTuple.setTupleMetaData(Size.STRINGSIZE,(short)1, fldOff);
		  columnTuple.setStrFld(1, strVal);
	  }

	  return hfColumns[column-1].updateRecord(tid.recordIDs[column-1], columnTuple);
	  }catch (Exception e)	{
		  e.printStackTrace();
	  }
	  return false;
  }
  boolean createBTreeIndex(int column)
  {
	  return true;
  }
  boolean createBitMapIndex(int columnNo, KeyClass value)
  {
	  return true;
  }
  public boolean markTupleDeleted(TID tid)
  {
	  byte[] deletedTuple = new byte[tupleLength];
	  byte[] deletedTids = new byte[numColumns*(2*INTSIZE)];
	  int i = 0;
	  int offset = 0;
	  int tidsOffset = 0;
	  short[] fldOff = {0};
	  Tuple delTuple;
	  try{
	  for (AttrType attr: type)
	  {
		if(attr.attrType == AttrType.attrInteger)
		{
			delTuple = hfColumns[i].getRecord(tid.recordIDs[i]);
			delTuple.setTupleMetaData(INTSIZE, (short)1, fldOff);
			Convert.setIntValue(tid.recordIDs[i].pageNo.pid, tidsOffset, deletedTids);
			Convert.setIntValue(tid.recordIDs[i].slotNo, tidsOffset + INTSIZE, deletedTids);
			
			Convert.setIntValue(delTuple.getIntFld(1), offset, deletedTuple);
			offset = offset + INTSIZE;
			tidsOffset = tidsOffset + (2 * INTSIZE);
			if(!hfColumns[i].deleteRecord(tid.recordIDs[i]))
				return false;
			i++;
		}
		else if(attr.attrType == AttrType.attrString)
		{
			delTuple = hfColumns[i].getRecord(tid.recordIDs[i]);
			delTuple.setTupleMetaData(Size.STRINGSIZE, (short)1, fldOff);
			Convert.setIntValue(tid.recordIDs[i].pageNo.pid, tidsOffset, deletedTids);
			Convert.setIntValue(tid.recordIDs[i].slotNo, tidsOffset + INTSIZE, deletedTids);
			
			Convert.setStrValue(delTuple.getStrFld(1), offset, deletedTuple);
			offset = offset + Size.STRINGSIZE;
			tidsOffset = tidsOffset + (2 * INTSIZE);
			if(!hfColumns[i].deleteRecord(tid.recordIDs[i]))
				return false;
			i++;
		}
	  }
		deletedTIDsHeapFile.insertRecord(deletedTids);
		deletedTIDs.add(deleteDump.insertRecord(deletedTuple));
		System.out.println("Tuple Marked Deleted");
		System.out.println("Total Number of Records Dumped in the DeleteDump File are "+deleteDump.getRecCnt());
		deleteCount++;
	  return true;
  	}catch (Exception e)
  	{
  		e.printStackTrace();
  	}
  	return false;
  }
  
  public void showDeleteDump() throws InvalidTupleSizeException, IOException
  {
	  System.out.println("Displaying the Delete Dump File");
	  Scan s = deleteDump.openScan();
	  RID rid = new RID();
	  Tuple t = new Tuple();
	   t = s.getNext(rid);
	   if (t!=null)
	   {
		   
		   while (t != null)
		   {
			   t.setTupleMetaData(this.getTupleLength(),(short) this.getnumColumns(), this.getFldOffset());
			   t.print(type);
			   t=s.getNext(rid);
		   }
	   }
	   else
	   {
		   System.out.println("Delete Dump File Empty and deleted records are purged");
	   }
  }
  
  public short[] getFldOffset()
	{
		short[] fldOffset = new short[this.getnumColumns()];
		
		for (int j = 0, offset = 0; j <this.getnumColumns() ; j++)
		{
			if (this.type[j].attrType == AttrType.attrString)
			{
				
				fldOffset[j] = (short) offset;
				offset = offset + Size.STRINGSIZE;
			}
			if (this.type[j].attrType == AttrType.attrInteger)
			{
				fldOffset[j] = (short) offset;
				offset = offset + INTSIZE;
			}
			
		}
		return fldOffset;
	}
  
  public ArrayList <RID> getDeletedRIDs(String fileName)
  {
	  ArrayList <RID> rid = new ArrayList<RID>();
	  try
	  {
		  
		  deleteDump = new Heapfile(fileName+".del");
		  deleteCount = deletedTIDsHeapFile.getRecCnt();
		  Tuple tuple = new Tuple();
		  RID r = new RID();
		  TID tid = new TID();
		  int offset = 0;
		  tid.recordIDs = new RID[numColumns];
	      for(int i =0; i< numColumns;i++)
	      {
	    	  tid.recordIDs[i] = new RID();
	      }
	      
		Scan scan = deletedTIDsHeapFile.openScan();
		Tuple t = new Tuple();
		System.out.println("record count"+deletedTIDsHeapFile.getRecCnt());
		byte [] b ;
		t=scan.getNext(r);
		while (t != null)
		{		
			b = t.getData();
			r = new RID();			
			for(int i =0 ; i < numColumns ; i++)
			{
				//System.out.println("offset" + offset);
				r.pageNo.pid = Convert.getIntValue(offset, b);
				r.slotNo = Convert.getIntValue(offset + INTSIZE, b);
				offset = offset + (2*INTSIZE);
			}
			rid.add(r);
			offset = 0;
			t=scan.getNext(r);
			
		}
		System.out.println("deletecount"+deleteCount);
	  }
	  catch (Exception e)
	  {
		  e.printStackTrace();
	  }
	  return rid;
  }
  
  public boolean purgeAllDeletedTuples()
  {
	  try {
		  deleteCount= deleteDump.getRecCnt();
		  int count = deleteCount;
	  for(int i =0 ; i< count; i++)
	  {
		  if(deletedTIDs.size() == 0)
				break;
		if(!deleteDump.deleteRecord(deletedTIDs.get(0)))
			return false;
		
		deletedTIDs.remove(0);
		
		deleteCount--;
	  }
	  return true;
	  } catch (Exception e)	{
		  e.printStackTrace();
	  }
	  return false;
  }
  
  public void deleteColumnarFile()
  {
	  try {
		 
		 for (Heapfile f: hfColumns)
		 {
			 f.deleteFile();
		 }
	  }
	  catch(Exception e)
	  {
		  e.printStackTrace();
	  }
  }
  
  public TID insertTuple(byte[] tupleptr)
  throws InvalidSlotNumberException,  
  	InvalidTupleSizeException,
  	SpaceNotAvailableException,
  	HFException,
  	HFBufMgrException,
  	HFDiskMgrException,
  	IOException
  {
	  int offset = 0;
	  TID tid = new TID();
	  tid.recordIDs = new RID[numColumns];
	  if(tupleptr.length >= MAX_SPACE)	{
		  throw new SpaceNotAvailableException(null, "Columnarfile: no available space");
	  }
	  int i = 0;
	  try {
	  for (AttrType attr: type)
	  {
		  if (attr.attrType == AttrType.attrInteger)	{
			  int intAttr = Convert.getIntValue(offset,tupleptr);
			  offset = offset + INTSIZE;
			  tid.recordIDs[i] = new RID();
			  byte[] intVal = new byte[INTSIZE];
			  Convert.setIntValue(intAttr, 0, intVal);
			  tid.recordIDs[i] = hfColumns[i].insertRecord(intVal);
		  }
		  if (attr.attrType == AttrType.attrString)	{
			  String strAttr = Convert.getStrValue(offset,tupleptr,Size.STRINGSIZE);
			  offset = offset + Size.STRINGSIZE;
			  tid.recordIDs[i] = new RID();
			  byte[] strVal = new byte[Size.STRINGSIZE];
			  Convert.setStrValue(strAttr, 0, strVal);
			  tid.recordIDs[i] = hfColumns[i].insertRecord(strVal);
		  }
		  
		  i++;
	  }
	  tid.numRIDs = i;
		 tid.pos = hfColumns[0].RidToPos(tid.recordIDs[0]);
	  }
	  catch (Exception e)	{
		  e.printStackTrace();
	  }
	  return tid;
  }
  
}// End of ColumnarFile 
