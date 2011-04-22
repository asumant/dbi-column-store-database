package global;

public class TID
{
	public int numRIDs;
	public int pos;
	public RID[] recordIDs;
	
	public TID () {}
	
	public TID(int numRIDs)
	{
		this.numRIDs=numRIDs;
	}
	
	public TID(int numRIDs, int pos)
	{
		this.numRIDs=numRIDs;
		this.pos=pos;		
	}
	
	public TID(int numRIDs, int pos, RID[] recordIDs)
	{
		this.numRIDs=numRIDs;
		this.pos=pos;
		this.recordIDs=recordIDs;
	}
	
	void copyTid(TID tid)
	{
		numRIDs= tid.numRIDs;
		this.pos = tid.pos;
		recordIDs=tid.recordIDs;
	}
	
	boolean equals(TID tid)
	{
		int i=0;
		if(tid==null)
			return false;

		if(numRIDs==tid.numRIDs && pos == tid.pos)
		{
			for(RID rid: recordIDs)
			{
				if(tid.recordIDs[i]!=null)
				{
					if(rid.slotNo == tid.recordIDs[i].slotNo && rid.pageNo.pid == tid.recordIDs[i++].pageNo.pid)
						continue;
					else
						return false;
				}
				else
					return false;
			}
			return true;
		}
		return false;	
	}
	
	void writeToByteArray(byte[] array, int offset)
	{
		try{
	      Convert.setIntValue ( numRIDs, offset, array);
	      Convert.setIntValue ( pos, offset+4, array);
	      offset=offset+8;
	      for(RID rec: recordIDs)
	      {
	      	//Convert.setIntValue ( rec, offset+8, array);
	        Convert.setIntValue ( rec.slotNo, offset, array);
	        Convert.setIntValue ( rec.pageNo.pid, offset+4, array);
	        offset=offset+8;
	      }
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	void setPosition(int pos)
	{
		this.pos = pos;
	}
	
	void setRID(int column, RID recordID)
	{
		this.recordIDs[column-1]=recordID;
	}

}
