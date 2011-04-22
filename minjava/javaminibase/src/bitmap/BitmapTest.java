package bitmap;

import global.AttrType;
import global.Convert;
import global.SystemDefs;
import global.TID;
import heap.Heapfile;
import heap.Tuple;

import java.io.IOException;

import columnar.Columnarfile;

public class BitmapTest {

	//This is added to substitute the struct construct in C++

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try {
/*		    SystemDefs sysdef = new SystemDefs("bitmap",100,100,"Clock");
			BitMapFile bFile=new BitMapFile("tempBitMap1", null, 1, new StringKey("ccccccccccccccccccccccccccccccccccccccc"), 1);
			Heapfile hf=new Heapfile("hTemp");
			System.out.println("Creating first HeapFile----------------------------------------------------------------------------");
			for(int i=0;i<50;i++)
			{
				byte []b=new byte[60];
				if(i%3==0)
					Convert.setStrValue("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 0, b);
				else if(i%5==0)
					Convert.setStrValue("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 0, b);
				else if(i%7==0)
					Convert.setStrValue("ccccccccccccccccccccccccccccccccccccccc", 0, b);
				else if(i%11==0)
					Convert.setStrValue("ddddddddddddddddddddddddddddddddddddddd", 0, b);
				else
					Convert.setStrValue("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", 0, b);
				hf.insertRecord(b);
			}
			
			System.out.println("\n\n\n---------------------------------------------------------------------------------");
			Heapfile hf1=new Heapfile("hTemp1");
			System.out.println("Creating second HeapFile----------------------------------------------------------------------------");
			for(int i=0;i<100;i++)
			{
				byte []b=new byte[60];
				if(i%5==0)
					Convert.setStrValue("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 0, b);
				else if(i%3==0)
					Convert.setStrValue("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 0, b);
				else if(i%7==0)
					Convert.setStrValue("ccccccccccccccccccccccccccccccccccccccc", 0, b);
				else if(i%11==0)
					Convert.setStrValue("ddddddddddddddddddddddddddddddddddddddd", 0, b);
				else
					Convert.setStrValue("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", 0, b);
				hf1.insertRecord(b);
			}
			
			System.out.println("\n\n\n---------------------------------------------------------------------------------");
			Heapfile hf2=new Heapfile("hTemp2");
			System.out.println("Creating third HeapFile----------------------------------------------------------------------------");
			for(int i=0;i<100;i++)
			{
				byte []b=new byte[60];
				if(i%5==0)
					Convert.setStrValue("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 0, b);
				else if(i%3==0)
					Convert.setStrValue("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 0, b);
				else if(i%7==0)
					Convert.setStrValue("ccccccccccccccccccccccccccccccccccccccc", 0, b);
				else if(i%11==0)
					Convert.setStrValue("ddddddddddddddddddddddddddddddddddddddd", 0, b);
				else
					Convert.setStrValue("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", 0, b);
				hf2.insertRecord(b);
			}
			System.out.println("\n\n\n---------------------------------------------------------------------------------");
			System.out.println("before create (BMF)....");
			bFile.createBitMap(hf);
			System.out.println("after create....");
			System.out.println("\n\n\n---------------------------------------------------------------------------------");

			BitMapFile resBf=new BitMapFile("resTemp", null, 1, new StringKey(""), 1);
			BitMapFile bf1=new BitMapFile("tempBitMap2", null, 1, new StringKey("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), 1);
			BitMapFile bf2=new BitMapFile("tempBitMap3", null, 1, new StringKey("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), 1);
			System.out.println("before print....");
			bFile.printBitMapFile();
			System.out.println("after print....");

			System.out.println("\n\n\n---------------------------------------------------------------------------------");
			System.out.println("before create (BMF1)....");
			bf1.createBitMap(hf1);
			System.out.println("after create....");
			System.out.println("\n\n\n---------------------------------------------------------------------------------");
			
			System.out.println("\n\n\n---------------------------------------------------------------------------------");
			System.out.println("before create (BMF2)....");
			bf2.createBitMap(hf2);
			System.out.println("after create....");
			System.out.println("\n\n\n---------------------------------------------------------------------------------");

			BitMapFile.bitwiseANDBitmaps(bf1, bf2, resBf);
			System.out.println("\n\n\n---------------------------------------------------------------------------------");
			System.out.println("Resultant Bitmap:");
			resBf.printBitMapFile();
			System.out.println("End Resultant Bitmap");
			System.out.println("\n\n\n---------------------------------------------------------------------------------");
			
			bFile.insert(46);
			System.out.println("After insert....");
			bFile.printBitMapFile();
			System.out.println("end....");
			System.out.println("\n\n\n---------------------------------------------------------------------------------");
			
			bFile.delete(49);
			System.out.println("After delete....");
			bFile.printBitMapFile();
			System.out.println("end....");
			System.out.println("\n\n\n---------------------------------------------------------------------------------");
			
			bFile.getTuplesBitMap(0, hf);
			bFile.close();
			bf1.close();
			bf2.close();
			hf1.deleteFile();
			hf2.deleteFile();
			hf.deleteFile();
			
			System.out.println("number of unpinned buffers: "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
		    System.out.println("number of buffers: "+SystemDefs.JavabaseBM.getNumBuffers());*/
			SystemDefs sysdef = new SystemDefs("bitmap",100,100,"Clock");
		    System.out.println ("\n  Test 1: Insert and scan fixed-size records\n");
		    final boolean OK = true;
		    final boolean FAIL = false;
		    int choice=100;
		    final int reclen = 128;
		    
		    boolean status = OK;
		    TID tid = new TID();
		    Columnarfile f = null;
		    
		    AttrType[] types = new AttrType[4];
		    
		    types[0] = new AttrType(AttrType.attrInteger);
		    types[1] = new AttrType(AttrType.attrString);
		    types[2] = new AttrType(AttrType.attrString);
		    types[3] = new AttrType(AttrType.attrInteger);
		    
		    System.out.println ("  - Create a columnar file\n");
		    try {
		      f = new Columnarfile("file_99", 4, types);
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
			DummyRec rec = new DummyRec(reclen);
			rec.ival1 = i;
			rec.ival2 = i+1;
			rec.test = "tuple" + i;
			rec.name = "record" + i%3;

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
		    
			BitMapFile bmf4=new BitMapFile("bmf4", f, 2, new StringKey("record1"), 1);
			bmf4.createBitMap(f.gethfColumns()[2]);
			bmf4.printBitMapFile();
			
		    
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

	}
}	
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


