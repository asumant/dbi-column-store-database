package columnar;

import heap.*;
import global.Convert;
import global.GlobalConst;
import global.RID;
import global.Size;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;

import java.io.IOException;

import columnar.ColumnarFileInfo;

public class ColumnarFileInfo implements GlobalConst	{

		private String columnarFileName;	//	STRINGSIZE * no of columns
		private int numColumns;				//	INTSIZE
		private int tupleLength;			//	INTSIZE
		public static int size;
		public int stringSize = 0;	
		private int 	[] attributeType;	//	INTSIZE  * no of columns
		private String 	[] heapFileNames;	//	STRINGSIZE * no of columns
		private String 	[] columnNames; //	60 * no of columns
		private byte	[] data;				
			
		
		public int getStringSize(Tuple _atuple) {
			try
			{
			data = _atuple.returnTupleByteArray();
			stringSize = Convert.getIntValue(0, data);
		    System.out.println("Unmarshalling : getting the String Size "+stringSize+" in the byte array at offset 0");
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
			return stringSize;
		}

		public void setStringSize(int stringSize) {
			this.stringSize = stringSize;
		}
		
		public String[] getHeapFileNames() {
			return heapFileNames;
		}

		public void setHeapFileNames(String heapFileNames, int index) {
			this.heapFileNames[index] = heapFileNames;
		}
		
		public String[] getColumnNames() {
			return columnNames;
		}

		public void setColumnNames(String columnNames, int index) {
			this.columnNames[index] = columnNames;
		}
					
		public String getColumnarFileName() {
			return columnarFileName;
		}

		public void setColumnarFileName(String columnarFileName) {
			this.columnarFileName = columnarFileName;
		}

		public int[] getAttributeType() {
			return attributeType;
		}

		public void setAttributeType(int attributeType, int index) {
			this.attributeType[index] = attributeType;
		}

		public int getNumColumns() {
			return numColumns;
		}

		public void setNumColumns(int numColumns) {
			this.numColumns = numColumns;
		}

		public int getTupleLength() {
			return tupleLength;
		}

		public void setTupleLength(int tupleLength) {
			this.tupleLength = tupleLength;
		}

		public byte[] getData() {
			return data;
		}

		public void setData(byte[] data) {
			this.data = data;
		}

	
	  public ColumnarFileInfo(String columnarFileName, int numOfCol)	  {  
		
		size = Size.STRINGSIZE * 2 * numOfCol + INTSIZE * 2 + INTSIZE * numOfCol + Size.STRINGSIZE +INTSIZE;
		this.data = new byte[size];		
	    this.numColumns = numOfCol;
	    this.tupleLength = 0;
	    this.attributeType = new int[numOfCol];
	    this.heapFileNames = new String[numOfCol];
	    this.columnNames = new String[numOfCol];
	    this.columnarFileName = columnarFileName;
	    	    
	  }
	  
	  public ColumnarFileInfo (String name)		{
		  	
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
		  
	  }
	 
	  public ColumnarFileInfo(byte[] array)	  {
		  
		  data = array;
	  }
	      
	   public byte [] returnByteArray()	   {
		   
	     return data;
	   }
	   public ColumnarFileInfo()
	   {
	   }
	   	     
	  public void getColumnarFileInfo(Tuple _atuple) throws InvalidTupleSizeException, IOException	{   
	        	 
	    	 data = _atuple.returnTupleByteArray();
		     this.columnarFileName = Convert.getStrValue(INTSIZE, data,Size.STRINGSIZE);
		     System.out.println("Unmarshalling : getting the columnarfile name value "+this.columnarFileName+" in the byte array at offset 0");
		     this.numColumns = Convert.getIntValue(Size.STRINGSIZE+INTSIZE, data);
		     System.out.println("Unmarshalling : getting the num of columns value "+numColumns+" in the byte array at offset "+ Size.STRINGSIZE+INTSIZE);
		     this.tupleLength = Convert.getIntValue(Size.STRINGSIZE+INTSIZE+INTSIZE, data);
		     System.out.println("Unmarshalling : getting the tuple length value "+tupleLength+" in the byte array at offset "+ Size.STRINGSIZE+INTSIZE+INTSIZE);
		     
		     this.heapFileNames = new String[numColumns];
		     this.columnNames = new String[numColumns];
		     this.attributeType = new int[numColumns];
		     
		     int offset = Size.STRINGSIZE + (3 * INTSIZE);
		     for (int i = 0 ; i < this.numColumns ; i++)
		     {
		    	 this.attributeType[i] = Convert.getIntValue(offset, data);
		    	 System.out.println("Unmarshalling : getting the attribute value "+this.attributeType[i]+" in the byte array at offset "+offset);
		    	 this.columnNames[i] = Convert.getStrValue(offset + INTSIZE, data,Size.STRINGSIZE);
		    	 System.out.println("Unmarshalling : getting the columnname value "+this.columnNames[i]+" in the byte array at offset "+(offset + INTSIZE));
		    	 this.heapFileNames[i] = Convert.getStrValue(offset + INTSIZE + Size.STRINGSIZE, data,Size.STRINGSIZE);
		    	 System.out.println("Unmarshalling : getting the heapfilename value "+this.heapFileNames[i]+" in the byte array at offset "+(offset + (INTSIZE + Size.STRINGSIZE)));
		    	 offset += (2* Size.STRINGSIZE)+INTSIZE;
		     }
	     
	  }
	  
	  
	  public Tuple convertToTuple()
	       throws IOException	{
		  
		  System.out.println("Marshalling : setting the numColumns value "+stringSize+" in the byte array at offset 0");
		  Convert.setIntValue(stringSize, 0, data);
		  System.out.println("Marshalling : setting the columnar file value "+this.getColumnarFileName()+" in the byte array at offset "+INTSIZE);
		  Convert.setStrValue(this.getColumnarFileName(), INTSIZE, data);
		  System.out.println("Marshalling : setting the numColumns value "+numColumns+" in the byte array at offset "+ Size.STRINGSIZE+INTSIZE);
		  Convert.setIntValue(numColumns, Size.STRINGSIZE+INTSIZE, data);
		  System.out.println("Marshalling : setting the tuplelength value "+tupleLength+" in the byte array at offset "+(Size.STRINGSIZE+INTSIZE+INTSIZE));
		  Convert.setIntValue(tupleLength, (Size.STRINGSIZE+INTSIZE+INTSIZE), data);
		  
		  int offset = Size.STRINGSIZE + (3 * INTSIZE);
		  
		  for (int i = 0 ; i < numColumns ; i++)	{
	   
	    	System.out.println("Marshalling : setting the attribute value "+this.attributeType[i]+" in the byte array at offset "+ offset);
	    	Convert.setIntValue(this.attributeType[i], offset, data);
	    	System.out.println("Marshalling : setting the columnname value "+this.columnNames[i]+" in the byte array at offset "+(offset+INTSIZE));
	    	Convert.setStrValue(this.columnNames[i], offset+INTSIZE, data);
	    	System.out.println("Marshalling : setting the heapfilename value "+this.heapFileNames[i]+" in the byte array at offset "+(offset+(INTSIZE+ Size.STRINGSIZE)));
	    	Convert.setStrValue(this.heapFileNames[i], offset+(INTSIZE+ Size.STRINGSIZE), data);
	    	offset += (2 * Size.STRINGSIZE) +INTSIZE;
	    	
	    }
	    
	    Tuple atuple = new Tuple(data, 0, size); 
	    return atuple;
	  }
	  
	    
	  
	  public void flushToTuple() throws IOException	  {
		  
		  	Convert.setStrValue(columnarFileName, 0, data);
		    int offset = Size.STRINGSIZE;
		    for (int i = 0 ; i < numColumns ; i++)	{
		    	
		    	Convert.setIntValue(this.attributeType[i], offset, data);
		    	Convert.setStrValue(this.heapFileNames[i], offset+INTSIZE, data);
		    	offset += Size.STRINGSIZE;
		    	
		    }
	  }
}
