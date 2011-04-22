package index;

import java.io.IOException;

import bitmap.BitMapFile;
import bitmap.ConstructPageException;
import bitmap.GetFileEntryException;
import bitmap.PinPageException;
import btree.BTFileScan;
import btree.BTreeFile;
import btree.IndexFile;
import btree.IndexFileScan;
import btree.IntegerKey;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.StringKey;
import heap.FileAlreadyDeletedException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.PredEval;
import iterator.Projection;
import iterator.UnknownKeyTypeException;
import global.AttrType;
import global.IndexType;
import global.RID;

public class ColumnarIndexScan  extends Iterator{
	
	private AttrType type;
	private IndexType index;
	private IndexFile     indFile;
	private IndexFileScan indScan;
	private BitMapFile bmf=null;
	private BTreeFile btf=null;
	private Scan bitMapScan=null;

//	public FldSpec[]      perm_mat;
	private AttrType    _type;
	private short       _s_size=0; 
//	private CondExpr[]    _selects;
	private int           _noInFlds;
//	private int           _noOutFlds;
	private Heapfile      f;
	private Tuple         tuple1;
	private Tuple         Jtuple;
//	private int           t1_size;
	private boolean       index_only;    
	private CondExpr[] selects;
	private String relName;
	public String getRelName() {
		return relName;
	}

	public String getIndName() {
		return indName;
	}

	private String indName;
	public CondExpr[] getSelects() {
		return selects;
	}

	public void setSelects(CondExpr[] selects) {
		this.selects = selects;
	}

	public AttrType getType() {
		return type;
	}

	public void setType(AttrType type) {
		this.type = type;
	}

	public IndexType getIndex() {
		return index;
	}

	public void setIndex(IndexType index) {
		this.index = index;
	}


	
	public ColumnarIndexScan(IndexType index,
			String relName,
			String indName,
			AttrType type,
			short str_sizes,
			CondExpr[] selects,
			boolean indexOnly) throws IndexException, UnknownIndexTypeException
	{
	    index_only = indexOnly;  
	    _type = type;
	    _s_size= str_sizes;
	    _noInFlds=1;
	    try {
			f=new Heapfile(relName);
		} catch (Exception e) {
			e.printStackTrace();
		}

		switch(index.indexType) {

		case IndexType.B_Index:
			try {
				btf = new BTreeFile(indName); 
				indScan = (BTFileScan) IndexUtils.BTree_scan(selects, btf);
			}
			catch (Exception e) {
				throw new IndexException(e, "IndexScan.java: BTreeFile exceptions caught from IndexUtils.BTree_scan().");
			}
			break;

		case IndexType.Bitmap:
			try {
				bmf=new BitMapFile(indName);
				bitMapScan=bmf.openScan();
			} catch (Exception e) {
				throw new IndexException(e, "BitMap creation Error!!");
			}	    	
			break;

		case IndexType.None:
			break;

		default:
			throw new UnknownIndexTypeException("Only BTree and Bitmap indexes are supported so far");

		}

		this.setIndex(index);
		this.setSelects(selects);
		this.setType(type);
	}
	
	  public Tuple get_next() 
	    throws IndexException, 
		   UnknownKeyTypeException,
		   IOException
	  {
	    RID rid=null;
	    KeyDataEntry nextentry = null;
	    
	    if(btf!=null)
	    {
	    try {
	      nextentry = indScan.get_next();
	    }
	    catch (Exception e) {
	      throw new IndexException(e, "IndexScan.java: BTree error");
	    }	  
	    
	      if (index_only) {
		// only need to return the key 

		AttrType[] attrType = new AttrType[1];
		short[] s_sizes = new short[1];
		
		if (_type.attrType == AttrType.attrInteger) {
		  attrType[0] = new AttrType(AttrType.attrInteger);
		  try {
		    Jtuple.setHdr((short) 1, attrType, s_sizes);
		  }
		  catch (Exception e) {
		    throw new IndexException(e, "IndexScan.java: Heapfile error");
		  }
		  
		  try {
		    Jtuple.setIntFld(1, ((IntegerKey)nextentry.key).getKey().intValue());
		  }
		  catch (Exception e) {
		    throw new IndexException(e, "IndexScan.java: Heapfile error");
		  }	  
		}
		else if (_type.attrType == AttrType.attrString) {
		  
		  attrType[0] = new AttrType(AttrType.attrString);
		  // calculate string size of _fldNum
		  int count = 0;
		  for (int i=0; i<1; i++) {
		    if (_type.attrType == AttrType.attrString)
		      count ++;
		  } 
		  s_sizes[0] = _s_size;
		  
		  try {
		    Jtuple.setHdr((short) 1, attrType, s_sizes);
		  }
		  catch (Exception e) {
		    throw new IndexException(e, "IndexScan.java: Heapfile error");
		  }
		  
		  try {
		    Jtuple.setStrFld(1, ((StringKey)nextentry.key).getKey());
		  }
		  catch (Exception e) {
		    throw new IndexException(e, "IndexScan.java: Heapfile error");
		  }	  
		}
		else {
		  // attrReal not supported for now
		  throw new UnknownKeyTypeException("Only Integer and String keys are supported so far"); 
		}
		return Jtuple;
	      }
	      
	      rid = ((LeafData)nextentry.data).getData();
	      try {
	    	  tuple1 = f.getRecord(rid);
	      }
	      catch (Exception e) {
	    	  throw new IndexException(e, "IndexScan.java: getRecord failed");
	      }
	      
	      
	      
	      return tuple1;
	    
	    }
	    else if(bmf!=null)
	    {
	    	try {
				Tuple t=bitMapScan.getNext(rid);
				return t;
			} catch (Exception e) {
				e.printStackTrace();
			}
	    }
	    
	    return null; 
	  }

	  public void close() throws IOException, IndexException
	  {
	    if (!closeFlag) {
	      if (indScan instanceof BTFileScan) {
		try {
		  ((BTFileScan)indScan).DestroyBTreeFileScan();
		}
		catch(Exception e) {
		  throw new IndexException(e, "BTree error in destroying index scan.");
		}
	      }
			else if(bmf!=null)
			{
				try {
					bmf.deleteFile();
				} catch (InvalidSlotNumberException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (FileAlreadyDeletedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InvalidTupleSizeException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (HFBufMgrException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (HFDiskMgrException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

	      
	      closeFlag = true; 
	    }
	  }
}
