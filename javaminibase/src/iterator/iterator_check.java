package iterator;

import index.IndexException;
import index.IndexScan;
import index.UnknownIndexTypeException;

import java.io.IOException;

import btree.AddFileEntryException;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import bufmgr.PageNotReadException;

import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.SpaceNotAvailableException;
import heap.Tuple;

public class iterator_check {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws HFDiskMgrException 
	 * @throws HFBufMgrException 
	 * @throws HFException 
	 * @throws InvalidTupleSizeException 
	 * @throws InvalidTypeException 
	 * @throws FieldNumberOutOfBoundException 
	 * @throws SpaceNotAvailableException 
	 * @throws InvalidSlotNumberException 
	 * @throws UnknownIndexTypeException 
	 * @throws IndexException 
	 * @throws AddFileEntryException 
	 * @throws ConstructPageException 
	 * @throws GetFileEntryException 
	 * @throws UnknownKeyTypeException 
	 * @throws InvalidRelation 
	 * @throws TupleUtilsException 
	 * @throws FileScanException 
	 * @throws WrongPermat 
	 * @throws UnknowAttrType 
	 * @throws PredEvalException 
	 * @throws PageNotReadException 
	 * @throws JoinsException 
	 */
	public  Heapfile f2;
	iterator_check() throws HFException, HFBufMgrException, HFDiskMgrException, IOException
	{
		f2=new Heapfile("check.in");
	}
	public void insert(Tuple t) throws InvalidSlotNumberException, InvalidTupleSizeException, SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException, IOException
	{
	//	
	}
	public static void main(String[] args) throws HFException, HFBufMgrException, HFDiskMgrException, IOException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException, InvalidSlotNumberException, SpaceNotAvailableException, IndexException, UnknownIndexTypeException, GetFileEntryException, ConstructPageException, AddFileEntryException, UnknownKeyTypeException, FileScanException, TupleUtilsException, InvalidRelation, JoinsException, PageNotReadException, PredEvalException, UnknowAttrType, WrongPermat {
		// TODO Auto-generated method stub
		Tuple t=new Tuple();
		iterator_check ic=new iterator_check();
		AttrType[] attr=new AttrType[2];
		attr[0]=new AttrType(AttrType.attrString);
		attr[1]=new AttrType(AttrType.attrReal);
		short[] shar=new short[1];
		shar[0]=30;
	    t.setHdr((short)2,attr,shar);
	    t.setStrFld(1,"vishnu");
	    t.setFloFld(2,1.0f);
	ic.insert(t);
	t.setStrFld(1,"viru");
	t.setFloFld(2,0.90f);
	ic.insert(t);
	//f2.insertRecord(t.getTupleByteArray());
	FldSpec[] fld=new FldSpec[2];
	fld[0]=new  FldSpec(new RelSpec(RelSpec.outer),1);
	fld[1]=new  FldSpec(new RelSpec(RelSpec.outer),2);
	CondExpr[] expr = new CondExpr[2];
	expr[0] = new CondExpr();
	expr[0].op = new AttrOperator(AttrOperator.aopEQ);
	expr[0].type1 = new AttrType(AttrType.attrSymbol);
	expr[0].type2 = new AttrType(AttrType.attrString);
	expr[0].next = null;
	expr[1] = null;// while((tup=sortiter1.get_next())!=null)// while((tup=sortiter1.get_next())!=null)
	expr[0].operand2.string = "vishnu";
	expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
			1);
	BTreeFile checktr = new  BTreeFile("check.bt", AttrType.attrString, 50, 1);
	// FileScan F1=new FileScan("score.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);
     FileScan f1=new FileScan("check.in",attr,shar,(short)2,2,fld,null);	
	IndexScan x=new IndexScan(new IndexType(IndexType.B_Index),"check.in","check.bt" , attr, shar, 2, 2,fld , null, 0, false);
	//BTreeFile btscores = new  BTreeFile("scored.bt", AttrType.attrString, 50, 1);
//	iscan = new IndexScan(b_index, "scored.in","scored.bt" ,
//			sumAttr, heapstr, sumlen,sumlen,projection, expr,
//				1, false);//
	System.out.println("printing value");
	Tuple tu=new Tuple();
	tu.setHdr((short)2, attr, shar);
	tu.tupleCopy(f1.get_next());
	tu.print(attr);
	
	
	

	}

}
