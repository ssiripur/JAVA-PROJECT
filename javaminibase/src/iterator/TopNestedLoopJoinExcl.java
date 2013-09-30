package iterator;

import global.AttrType;
import global.TupleOrder;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

import java.io.IOException;

import bufmgr.PageNotReadException;

public class TopNestedLoopJoinExcl extends NestedLoopsJoins1 {
 Iterator iter;
 int num1;
 AttrType[] resAttrTypes;
	public TopNestedLoopJoinExcl(AttrType[] in1, int len_in1,
			short[] t1_str_sizes, AttrType[] in2, int len_in2,
			short[] t2_str_sizes, int amt_of_mem, Iterator am1,
			String relationName, CondExpr[] outFilter,
			FldSpec[] proj_list, int n_out_flds,int num) throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		super(in1, len_in1, t1_str_sizes, in2, len_in2, t2_str_sizes,
				amt_of_mem, am1, relationName, outFilter, null,
				proj_list, n_out_flds);
		// TODO Auto-generated constructor stub
		System.out.println("super called");
		num1=num;
//		int num2=0;
//		TupleOrder o=new TupleOrder(TupleOrder.Descending);
//	resAttrTypes=new AttrType[n_out_flds];
//		for(int i=0;i<n_out_flds;i++)
//		{
//			if(proj_list[i].relation.key==0)
//			{
//			resAttrTypes[i]=in1[proj_list[i].offset-1];
//			}
//			else
//			{
//				resAttrTypes[i]=in2[proj_list[i].offset-1];	
//			}
//		
//		}
//          Tuple t=new Tuple();
//          for(int i=0;i<5;i++)
//  		{
//  			this.get_next().print(resAttrTypes);
//  		}
////      

		iter = new Sort(this.Jtypes,(short)n_out_flds,this.t_size, this, n_out_flds,
					  new TupleOrder(TupleOrder.Descending),4, amt_of_mem);
		
		
	}

	public Tuple get_TopRanked() throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception
	{	
		Tuple t;
		while((num1!=0))
		{
			num1--;
			t=iter.get_next();
			if(t!=null)
			{
		//  Jtuple.tupleCopy(t);
	      System.out.println("printing data");
	      t.print(resAttrTypes);
	 //    System.out.println(t.getScore());
			return t;}
			else
			{
				return null;
			}
		}
		return null;
	}
	public void Close()
	{
		
	}
	

}
