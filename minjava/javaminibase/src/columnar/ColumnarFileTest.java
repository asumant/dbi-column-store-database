package columnar;

import global.AttrType;
import global.Convert;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import global.TID;
import heap.Tuple;

public class ColumnarFileTest implements GlobalConst {

	static int reclen = 128;
	public static void main(String args[])
	{
		SystemDefs sysdef = new SystemDefs("columnDb",100,100,"Clock");
		TID tid = new TID();
		AttrType [] at = new AttrType[4];
		String [] cn = new String [4];
		at[0] = new AttrType(AttrType.attrInteger);
		cn[0] = new String("abc");
		at[1] = new AttrType(AttrType.attrString);
		cn[1] = new String("def");
		at[2] = new AttrType(AttrType.attrString);
		cn[2] = new String("ghi");
		at[3] = new AttrType(AttrType.attrInteger);
		cn[3] = new String("jkl");
		Columnarfile cf = new Columnarfile("AAA",4,at);
		cf.setColumnNames(cn);
		cf.setColumnarFileInfo(60);
		 int choice = 10; 
		for (int i =0; i < choice ; i++) {
			
			//fixed length record
			DummyRec rec = new DummyRec(reclen);
			rec.ival1 = i;
			rec.ival2 = i+1;
			rec.test = "tuple" + i;
			rec.name = "record" + i;
			
			System.out.print(i);
			System.out.print(" "+(i+1));
			System.out.print(" tuple" + i);
			System.out.print(" record" + i);
			System.out.println();
			try {
				tid = cf.insertTuple(rec.toByteArray());
			}
			catch (Exception e) {
			 // status = FAIL;
			  System.err.println ("*** Error inserting record " + i + "\n");
			  e.printStackTrace();
			}

		}
		try
		{
			 tid.recordIDs = new RID[cf.getnumColumns()];
		      for(int i =0; i<cf.getnumColumns();i++)
		      {
		    	  tid.recordIDs[i] = new RID();
		      }
		      
			TupleScan scan = cf.openTupleScan();
			Tuple t = new Tuple();
			t = scan.getNext(tid);
			
			cf.markTupleDeleted(tid);
			t = scan.getNext(tid);
			cf.markTupleDeleted(tid);
			t = scan.getNext(tid);
			cf.markTupleDeleted(tid);
			cf.getDeletedRIDs("AAA");
			
			//db close
			cf.getColumnarFileInfo("AAA.hdr");
			
			//System.out.println(cf.getHeapfileForColumname("ghi").getRecCnt());
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
}
//This is added to substitute the struct construct in C++
class DummyRec  {
  
  //content of the record
  public int    ival1; 
  public String test;      
  public String name;
  public int	ival2;

  //length under control
  private int reclen;
  
  private byte[]  data;

  /** Default constructor
   */
  public DummyRec() {}

  /** another constructor
   */
  public DummyRec (int _reclen) {
    setRecLen (_reclen);
    data = new byte[_reclen];
  }
  
  /** constructor: convert a byte array to DummyRec object.
   * @param arecord a byte array which represents the DummyRec object
   */
  public DummyRec(byte [] arecord) 
    throws java.io.IOException {
    setInt1Rec (arecord);
    setStr1Rec (arecord);
    setStr2Rec (arecord);
    setInt2Rec (arecord);
    data = arecord; 
    setRecLen(name.length());
  }

  /** constructor: translate a tuple to a DummyRec object
   *  it will make a copy of the data in the tuple
   * @param atuple: the input tuple
   */
  public DummyRec(Tuple _atuple) 
	throws java.io.IOException{   
    data = new byte[_atuple.getLength()];
    data = _atuple.getTupleByteArray();
    setRecLen(_atuple.getLength());
    
    setInt1Rec (data);
    setStr1Rec (data);
    setStr2Rec (data);
    setInt2Rec (data);
  }

  /** convert this class objcet to a byte array
   *  this is used when you want to write this object to a byte array
   */
  public byte [] toByteArray() 
    throws java.io.IOException {
    //    data = new byte[reclen];
    Convert.setIntValue (ival1, 0, data);
    Convert.setStrValue (test, 4, data);
    Convert.setStrValue (name, 64, data);
    Convert.setIntValue(ival2, 124, data);
    return data;
  }
  
  /** get the integer value out of the byte array and set it to
   *  the int value of the DummyRec object
   */
  public void setInt1Rec (byte[] _data) 
    throws java.io.IOException {
    ival1 = Convert.getIntValue (0, _data);
  }

  /** get the float value out of the byte array and set it to
   *  the float value of the DummyRec object
   */
  public void setStr1Rec (byte[] _data) 
    throws java.io.IOException {
    test = Convert.getStrValue (4, _data,60);
  }

  /** get the String value out of the byte array and set it to
   *  the float value of the HTDummyRecorHT object
   */
  public void setStr2Rec (byte[] _data) 
    throws java.io.IOException {
   // System.out.println("reclne= "+reclen);
   // System.out.println("data size "+_data.size());
    name = Convert.getStrValue (64, _data, 60);
  }
  
  public void setInt2Rec (byte[] _data) 
  throws java.io.IOException {
	  ival2 = Convert.getIntValue (124, _data);
  }
  //Other access methods to the size of the String field and 
  //the size of the record
  public void setRecLen (int size) {
    reclen = size;
  }
  
  public int getRecLength () {
    return reclen;
  }  
 }

