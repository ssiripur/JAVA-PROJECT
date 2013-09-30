package iterator;

import java.io.IOException;

import global.AttrType;
import global.RID;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.InvalidUpdateException;
import heap.SpaceNotAvailableException;
import heap.Tuple;

public class FileScan2_delete {

	public FileScan2_delete(
			
			
			
			) throws InvalidUpdateException, Exception {
		// TODO Auto-generated constructor stub
		Heapfile m=new Heapfile("raju.in");
		AttrType[][] attrTypes=new AttrType[4][5];
	attrTypes[0] = new AttrType[5];
	        attrTypes[0][0] = new AttrType(AttrType.attrInteger);
	        attrTypes[0][1] = new AttrType(AttrType.attrString);
	        attrTypes[0][2] = new AttrType(AttrType.attrString);
	        attrTypes[0][3] = new AttrType(AttrType.attrString);
	        attrTypes[0][4] = new AttrType(AttrType.attrReal);
	        
	       short[] strty={50,50,50};
	       short sumlen=5;
	       Tuple t=new  Tuple();
	       t.setHdr(sumlen, attrTypes[0], strty);
	       t.setIntFld(1, 34);
	       t.setStrFld(2, "sherry");
	       t.setStrFld(3, "btree");
	       t.setStrFld(4, "sehh");
	       t.setFloFld(5, 1.0f);
   	       RID r=  m.insertRecord(t.getTupleByteArray());
	       String re=r.pageNo+"-"+r.slotNo;
	       t.setStrFld(4, re);
	       System.out.println(r.pageNo);
	       System.out.println(r.slotNo);
	       m.updateRecord(r, t);
	       t.tupleCopy(m.getRecord(r));
	       t.print(attrTypes[0]);   
	        
		
	}

}
