package iterator;

import global.AttrType;
import global.RID;
import global.TID;
import heap.FieldNumberOutOfBoundException;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;

import java.io.IOException;

import columnar.Columnarfile;

import bufmgr.PageNotReadException;

public class ColumnarFileScan extends Iterator {

	private AttrType[] _in1;
	  private short in1_len;
	  private short[] s_sizes; 
	  private Columnarfile f;
	  private Scan scan;
	  private Tuple     tuple1;
	  private Tuple    Jtuple;
	  private int        t1_size;
	  private int nOutFlds;
	  private CondExpr[]  OutputFilter;
	  public FldSpec[] perm_mat;

	 

	  /**
	   *constructor
	   *@param file_name heapfile to be opened
	   *@param in1[]  array showing what the attributes of the input fields are. 
	   *@param s1_sizes[]  shows the length of the string fields.
	   *@param len_in1  number of attributes in the input tuple
	   *@param n_out_flds  number of fields in the out tuple
	   *@param proj_list  shows what input fields go where in the output tuple
	   *@param outFilter  select expressions
	   *@exception IOException some I/O fault
	   *@exception FileScanException exception from this class
	   *@exception TupleUtilsException exception from this class
	   *@exception InvalidRelation invalid relation 
	   */
	  public  ColumnarFileScan (String  file_name,
			    AttrType in1[],                
			    short s1_sizes[], 
			    short     len_in1,              
			    int n_out_flds,
			    FldSpec[] proj_list,
			    CondExpr[]  outFilter        		    
			    )
	    {
	      // implemented the logic in the Query Class
	    }
	  
	  /**
	   *@return shows what input fields go where in the output tuple
	   */
	  public FldSpec[] show()
	    {
	      return perm_mat;
	    }
	  
	  /**
	   *@return the result tuple
	   *@exception JoinsException some join exception
	   *@exception IOException I/O errors
	   *@exception InvalidTupleSizeException invalid tuple size
	   *@exception InvalidTypeException tuple type not valid
	   *@exception PageNotReadException exception from lower layer
	   *@exception PredEvalException exception from PredEval class
	   *@exception UnknowAttrType attribute type unknown
	   *@exception FieldNumberOutOfBoundException array out of bounds
	   *@exception WrongPermat exception for wrong FldSpec argument
	   */
	  public Tuple get_next()
	    throws JoinsException,
		   IOException,
		   InvalidTupleSizeException,
		   InvalidTypeException,
		   PageNotReadException, 
		   PredEvalException,
		   UnknowAttrType,
		   FieldNumberOutOfBoundException,
		   WrongPermat
	    {     
	      TID tid = new TID();
	      RID rid = new RID();
	      while(true) {
	    	  if((tuple1 =  scan.getNext(rid)) == null) {
		  return null;
		}
		
		tuple1.setHdr(in1_len, _in1, s_sizes);
		if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true){
		  Projection.Project(tuple1, _in1,  Jtuple, perm_mat, nOutFlds); 
		  return  Jtuple;
		}        
	      }
	    }

	  /**
	   *implement the abstract method close() from super class Iterator
	   *to finish cleaning up
	   */
	  public void close() 
	    {
	     
	      if (!closeFlag) {
		scan.closescan();
		closeFlag = true;
	      } 
	    }
}
