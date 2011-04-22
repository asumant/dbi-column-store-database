package columnar;

import global.*;
import heap.*;

public class TupleScan implements GlobalConst{
	
	private Scan[] hfScan;
	private Columnarfile currentCf;
	
	public TupleScan(Columnarfile cf)
	{
		int i = 0;
		currentCf = cf;
		hfScan = new Scan[cf.getnumColumns()];
		try {
		for (Heapfile hf: cf.hfColumns)
		{
			hfScan[i++] = hf.openScan();
		}
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
	public Tuple getNext(TID tid)
	{
		Tuple nextTuple = new Tuple(currentCf.getTupleLength());
		short[] str_sizes;
		int strCount = 0;
		short[] fldOffset = new short[currentCf.getTupleCnt()];
			
		for (int j = 0, offset = 0; j <currentCf.getnumColumns() ; j++)
		{
			if (currentCf.type[j].attrType == AttrType.attrString)
			{
				strCount++;
				fldOffset[j] = (short) offset;
				offset = offset + Size.STRINGSIZE;
			}
			if (currentCf.type[j].attrType == AttrType.attrInteger)
			{
				fldOffset[j] = (short) offset;
				offset = offset + INTSIZE;
			}
			
		}
		str_sizes = new short[strCount];
		for (short str: str_sizes)
			str = (short)Size.STRINGSIZE;
		
		try {
			
		nextTuple.setTupleMetaData(currentCf.getTupleLength(), (short)currentCf.getnumColumns(), fldOffset);
		
		int i = 0;
		
		for (Scan hf: hfScan)
		{
			if (currentCf.type[i].attrType == AttrType.attrInteger)	{
				Tuple t = hf.getNext(tid.recordIDs[i]);
				
				if (t==null)
					return null;
				
				short[] colOffset = {0};
				t.setTupleMetaData(INTSIZE, (short)1, colOffset);
								
				nextTuple.setIntFld(i+1, t.getIntFld(1));
			}
			if (currentCf.type[i].attrType == AttrType.attrString)	{
				Tuple t = hf.getNext(tid.recordIDs[i]);
				
				if(t == null)
					return null;
				
				short[] colOffset = {0};
				t.setTupleMetaData(Size.STRINGSIZE, (short)1, colOffset);
								
				nextTuple.setStrFld(i+1, t.getStrFld(1));
				strCount++;
			}
			i++;
		}
		
		} catch (Exception e){
			e.printStackTrace();
		}
		return nextTuple;
	}
	
	public boolean position (TID tid)
	{
		int i =0 ;
		try {
			for(Scan hf: hfScan)
			{
				if(!hf.position(tid.recordIDs[i++]))
					return false;
			}
			return true;
		} catch (Exception e){
			e.printStackTrace();
		}
		return false;
	}
	
	public void closetuplescan()
	{		
		for(Scan hf: hfScan)
			hf.closescan();
		
		hfScan = null;
		currentCf = null;
	}
}