package bitmap;

import global.Convert;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.*;
import global.*;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import tests.Delete_Query;
import tests.Query;

import diskmgr.Page;

import bitmap.AddFileEntryException;
import bitmap.BitMapHeaderPage;
import bitmap.DeleteFileEntryException;
import bitmap.FreePageException;
import bitmap.GetFileEntryException;
import bitmap.IndexFile;
import bitmap.PinPageException;
import bitmap.UnpinPageException;
import bitmap.BitMapHeaderPage;
import bitmap.ConstructPageException;
import bitmap.IteratorException;
import bitmap.NodeType;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import columnar.Columnarfile;


public class BitMapFile extends Heapfile{
  
  private final static int MAGIC0=1989;
  public static final int INVALID_PAGE = -1;
  
  private final static String lineSep=System.getProperty("line.separator");
  
  private static FileOutputStream fos;
  private static DataOutputStream trace;
  
  
  public static void traceFilename(String filename) 
  throws  IOException
  {
    
    fos=new FileOutputStream(filename);
    trace=new DataOutputStream(fos);
  }

  /** Stop tracing. And close trace file. 
   *@exception IOException error from the lower layer
   */
  public static void destroyTrace() 
  throws  IOException
  {
    if( trace != null) trace.close();
    if( fos != null ) fos.close();
    fos=null;
    trace=null;
  }

  private BitMapHeaderPage headerPage;
  private  PageId  headerPageId;
  private String  dbname;  
  private Columnarfile cFile;
  private int columnNo;
  private KeyClass value;

  public KeyClass getValue() {
	return value;
}

public void setValue(KeyClass value) {
	System.out.println("class of kc"+value.getClass());
	if(value instanceof IntegerKey)
		this.value = (IntegerKey)value;
	if(value instanceof StringKey)
		this.value = (StringKey)value;	
}

public Columnarfile getcFile() {
	return cFile;
}

public void setcFile(Columnarfile cFile) {
	this.cFile = cFile;
}

public int getColumnNo() {
	return columnNo;
}

public BitMapHeaderPage getHeaderPage() {
	    return headerPage;
	  }
	  
  private PageId get_file_entry(String filename)         
  throws GetFileEntryException
  {
	  try {
		  return SystemDefs.JavabaseDB.get_file_entry(filename);
	  }
	  catch (Exception e) {
		  e.printStackTrace();
		  throw new GetFileEntryException(e,"");
	  }
  }

  private Page pinPage(PageId pageno) throws PinPageException
  {
	  try {
	        Page page=new Page();
	        SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
	        return page;
	  }
	  catch (Exception e) {
		  	e.printStackTrace();
		  	throw new PinPageException(e,"");
	  }
  }
	  
  private void add_file_entry(String fileName, PageId pageno) 
  throws AddFileEntryException
  {
	  try 
	  {
		  SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
	  }
	  catch (Exception e) 
	  {
		  e.printStackTrace();
		  throw new AddFileEntryException(e,"");
	  }      
  }
	  
  private void unpinPage(PageId pageno) 
  throws UnpinPageException
  { 
	  try
	  {
		  SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);    
	  }
	  catch (Exception e) 
	  {
		  e.printStackTrace();
		  throw new UnpinPageException(e,"");
	  } 
  }
	  
  private void freePage(PageId pageno) 
  throws FreePageException
  {
	  try
	  {
		  SystemDefs.JavabaseBM.freePage(pageno);    
	  }
	  catch (Exception e) 
	  {
		  e.printStackTrace();
		  throw new FreePageException(e,"");
	  } 
  }
	
  private void delete_file_entry(String filename)
  throws DeleteFileEntryException
  {
	  try 
	  {
	      SystemDefs.JavabaseDB.delete_file_entry( filename );
	  }
	  catch (Exception e) 
	  {
		  e.printStackTrace();
		  throw new DeleteFileEntryException(e,"");
	  } 
  }
	  
  private void unpinPage(PageId pageno, boolean dirty) 
  throws UnpinPageException
  {
	  try
	  {
	      SystemDefs.JavabaseBM.unpinPage(pageno, dirty);  
	  }
	  catch (Exception e) 
	  {
		  e.printStackTrace();
		  throw new UnpinPageException(e,"");
	  }  
  }
  
  /**  BTreeFile class
   * an index file with given filename should already exist; this opens it.
   *@param filename the B+ tree file name. Input parameter.
   *@exception GetFileEntryException  can not ger the file from DB 
   *@exception PinPageException  failed when pin a page
   *@exception ConstructPageException   BT page constructor failed
 * @throws IOException 
 * @throws HFDiskMgrException 
 * @throws HFBufMgrException 
 * @throws HFException 
   */
  public BitMapFile(String filename)
    throws GetFileEntryException,PinPageException, 
	   ConstructPageException, HFException, HFBufMgrException, IOException, HFDiskMgrException        
    {      
	  super(filename);

//	  headerPageId=get_file_entry(filename);   
//	      headerPage= new  BitMapHeaderPage( headerPageId);  
      //dbname = new String(filename);
      /*
       *
       * - headerPageId is the PageId of this BTreeFile's header page;
       * - headerPage, headerPageId valid and pinned
       * - dbname contains a copy of the name of the database
       */
    }    
  
  public void setColumnNo(int columnNo)
  {
	  this.columnNo=columnNo;
  }
  
  
  /**
   *  if index file exists, open it; else create it.
   *@param filename file name. Input parameter.
   *@param keytype the type of key. Input parameter.
   *@param keysize the maximum size of a key. Input parameter.
   *@param delete_fashion full delete or naive delete. Input parameter.
   *           It is either DeleteFashion.NAIVE_DELETE or 
   *           DeleteFashion.FULL_DELETE.
   *@exception GetFileEntryException  can not get file
   *@exception ConstructPageException page constructor failed
   *@exception IOException error from lower layer
   *@exception AddFileEntryException can not add file into DB
 * @throws HFDiskMgrException 
 * @throws HFBufMgrException 
 * @throws HFException 
   */
  public BitMapFile(String filename, Columnarfile cFile,
		   int columnNo, KeyClass value, int deleteFashion)  
    throws GetFileEntryException, 
	   ConstructPageException,
	   IOException, 
	   AddFileEntryException, HFException, HFBufMgrException, HFDiskMgrException
    {
	  super(filename);
      headerPageId=get_file_entry(filename);
	  headerPage= new  BitMapHeaderPage(); 
	  headerPageId= headerPage.getPageId();
	  headerPage.set_magic0(MAGIC0);
	  headerPage.set_rootId(new PageId(INVALID_PAGE));
	  headerPage.set_keyType(new Character('0'));    
	  headerPage.set_maxKeySize(2);
	  headerPage.set_deleteFashion(deleteFashion);
	  headerPage.setType(NodeType.BTHEAD);
	  this.setcFile(cFile);
	  this.setColumnNo(columnNo);
	  this.setValue(value);
	  dbname=new String(filename);
    }

  /** Close the B+ tree file.  Unpin header page.
   *@exception PageUnpinnedException  error from the lower layer
   *@exception InvalidFrameNumberException  error from the lower layer
   *@exception HashEntryNotFoundException  error from the lower layer
   *@exception ReplacerException  error from the lower layer
   */
  public void close()
    throws PageUnpinnedException, 
	   InvalidFrameNumberException, 
	   HashEntryNotFoundException,
           ReplacerException
    {
      if ( headerPage!=null) {
	SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
	headerPage=null;
      }  
    }
  
  public ArrayList<Tuple> getTuplesBitMap(Heapfile curFile, AttrType []types)
  {
	  ArrayList<Tuple> result=new ArrayList<Tuple>();
	  try {
			Scan bScan=this.openScan();
			Tuple hTuple=new Tuple();
			Tuple bTuple=new Tuple();
			RID bRID=new RID();
			byte[] yes=new byte[2];
			byte[] no=new byte[2];
			Convert.setCharValue('0', 0, no);
			Convert.setCharValue('1', 0, yes);
			int position=1;
			while((bTuple=bScan.getNext(bRID))!=null)
			{
				if(Arrays.equals(bTuple.getData(),yes)==true)
				{
					Query.getProjection(position);
					System.out.print("current position: "+position);
				}
				position++;
			}
			bScan.closescan();
		  } 
		  catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		  }
	  return result;
  }

  public ArrayList<Tuple> deleteTuplesBitMap(Heapfile curFile, Columnarfile cf, AttrType []types)
  {
	  ArrayList<Tuple> result=new ArrayList<Tuple>();
	  try {
		Scan hScan= new Scan(curFile);
		Scan bScan=this.openScan();
		Tuple hTuple=new Tuple();
		Tuple bTuple=new Tuple();
		RID bRID=new RID();
		
		byte[] yes=new byte[2];
		byte[] no=new byte[2];
		Convert.setCharValue('0', 0, no);
		Convert.setCharValue('1', 0, yes);
		int position=1;
		TID tid=new TID();
		while((bTuple=bScan.getNext(bRID))!=null)
		{
			if(Arrays.equals(bTuple.getData(),yes)==true)
			{
				RID rid=curFile.PosToRid(position);
				hTuple=curFile.getRecord(rid);
				result.add(hTuple);
				if(hTuple.getLength()>4)
				{
					System.out.println(Convert.getStrValue(0, hTuple.getData(),hTuple.getLength()));
				}
				else
				{
					System.out.println(Convert.getIntValue(0, hTuple.getData()));
				}
				tid= Delete_Query.deleteProjection(position);
				cf.markTupleDeleted(tid);
			}
			position++;
		}
		hScan.closescan();
		bScan.closescan();
	  } 
	  catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	  }
	  return result;
  }

  public void printBitMapFile()
  {
	  try
	  {
		  Scan scanHf=this.openScan();
		  RID rid=new RID();
		  Tuple tScan=new Tuple();
		  byte[] yes=new byte[2];
		  byte[] no=new byte[2];
		  Convert.setCharValue('0', 0, no);
		  Convert.setCharValue('1', 0, yes);
		  System.out.println("Printing Bitmap contents: ");
		  int cnt=1;
		  while((tScan=scanHf.getNext(rid))!=null)
		  {
			  byte[] temp=tScan.getData();
			  if(Arrays.equals(temp,yes))
			  {
				  System.out.println("bitmap value at position '"+cnt+"': 1");
			  }
			  else if(Arrays.equals(temp,no))
			  {
				  System.out.println("bitmap value at position '"+cnt+"': 0");
			  }
			  cnt++;
		  }

	  }
	  catch(Exception e)
	  {
		  e.printStackTrace();
	  }
  }
  
  public static void bitwiseANDBitmaps(BitMapFile bf1, BitMapFile bf2, BitMapFile resBf)
  {
	  try
	  {
		  //Heapfile hf= this.getcFile().hfColumns[columnNo];
		  Scan scanBf1=bf1.openScan();
		  Scan scanBf2=bf2.openScan();
		  RID rid1=new RID();
		  RID rid2=new RID();
		  Tuple tScan1=new Tuple();
		  Tuple tScan2=new Tuple();
		  byte[] yes=new byte[2];
		  byte[] no=new byte[2];
		  Convert.setCharValue('0', 0, no);
		  Convert.setCharValue('1', 0, yes);
		  while((tScan1=scanBf1.getNext(rid1))!=null && (tScan2=scanBf2.getNext(rid2))!=null)
		  {
			  byte[] temp1=tScan1.getData();
			  byte[] temp2=tScan2.getData();
			  
			  //here we write the corresponding value to the BitMapFile object resBf and return the ANDed BitMap 
			  if(Arrays.equals(temp1,yes)&&Arrays.equals(temp2,yes))
			  {
				  resBf.insertRecord(yes);
				  System.out.println("match 1.....");
			  }
			  else
			  {
				  resBf.insertRecord(no);
				  System.out.println("no match 0.....");
			  }
		  }

	  }
	  catch(Exception e)
	  {
		  e.printStackTrace();
	  }	  
  }
  
  public void createBitMap(Heapfile hf)
  {
	  try
	  {
		  Scan scanHf=hf.openScan();
		  RID rid=new RID();
		  Tuple tScan=new Tuple();
		  byte[] yes=new byte[2];
		  byte[] no=new byte[2];
		  Convert.setCharValue('0', 0, no);
		  Convert.setCharValue('1', 0, yes);
		  int iTemp = 0;
		  String sTemp = "";
		  byte [] bTemp=null;
		  if (this.value instanceof IntegerKey)
		  {
			  iTemp = ((IntegerKey) this.value).getKey();
			  bTemp=new byte[4];
			  Convert.setIntValue(iTemp, 0, bTemp);
			  System.out.println("integer value detected..."+iTemp);
		  }
		  else if (this.value instanceof StringKey)
		  {
			  sTemp = ((StringKey) this.value).getKey();
			  bTemp=new byte[Size.STRINGSIZE];
			  Convert.setStrValue(sTemp, 0, bTemp);
			  System.out.println("string value detected...");
		  }
		  int cnt=0;
		  while((tScan=scanHf.getNext(rid))!=null)
		  {
			  byte[] temp=tScan.getData();
			  		  
			  if(Arrays.equals(temp,bTemp))
			  {
				  this.insertRecord(yes);
				  System.out.println("inserting 1......"+(cnt++));
			  }
			  else
			  {
				  this.insertRecord(no);
				  System.out.println("inserting 0......"+cnt);
			  }
		  }
		  scanHf.closescan();
	  }
	  catch(Exception e)
	  {
		  e.printStackTrace();
	  }
  }
  
  boolean delete(int position)
  {
	  boolean status=true;
	  try
	  {
		  //Heapfile hf= this.getcFile().hfColumns[columnNo];
		  Scan scanHf=this.openScan();
		  RID rid=new RID();
		  Tuple tScan=new Tuple();
		  int cnt=0;
		  while((tScan=scanHf.getNext(rid))!=null)
		  {
			  System.out.println("(inside delete)searching....");
			  if(cnt<position)
				  cnt++;
			  else
				  break;
		  }
		  byte [] tempData=tScan.getData();
		  Convert.setCharValue('0', 0, tempData);
		  tScan.setData(tempData);
		  status=this.updateRecord(rid, tScan);
	  }
	  catch(Exception e)
	  {
		  e.printStackTrace();
	  }	  
	  return status;
  }


  boolean insert(int position)
  {
	  boolean status=true;
	  try
	  {
		  //Heapfile hf= this.getcFile().hfColumns[columnNo];
		  Scan scanHf=this.openScan();
		  RID rid=new RID();
		  Tuple tScan=new Tuple();
		  int cnt=0;
		  while((tScan=scanHf.getNext(rid))!=null)
		  {
			  System.out.println("(inside insert)searching....");
			  if(cnt<position)
				  cnt++;
			  else
				  break;
		  }
		  byte [] tempData=tScan.getData();
		  Convert.setCharValue('1', 0, tempData);
		  tScan.setData(tempData);
		  status=this.updateRecord(rid, tScan);
	  }
	  catch(Exception e)
	  {
		  e.printStackTrace();
	  }	  
	  return status;
  }

  private void  updateHeader(PageId newRoot)
  throws   IOException, 
	     PinPageException,
	     UnpinPageException
  {
    
    BitMapHeaderPage header;
    PageId old_data;
    
    
    header= new BitMapHeaderPage( pinPage(headerPageId));
    
    old_data = headerPage.get_rootId();
    header.set_rootId( newRoot);
    
    // clock in dirty bit to bm so our dtor needn't have to worry about it
    unpinPage(headerPageId, true /* = DIRTY */ );
    
    
    // ASSERTIONS:
    // - headerPage, headerPageId valid, pinned and marked as dirty
    
  }

}
