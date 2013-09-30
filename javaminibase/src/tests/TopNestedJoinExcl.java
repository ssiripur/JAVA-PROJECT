/**
 * 
 */
package tests;

import java.io.IOException;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.TupleOrder;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.NestedLoopsJoins;
import iterator.NestedLoopsJoins1;
import iterator.PredEvalException;
import iterator.Sort;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;

/**
 * @author beck
 *
 */
public class TopNestedJoinExcl extends NestedLoopsJoins1 {

	Iterator p_i1;
	int num1;
	Tuple Jtuple;
	AttrType[] resAttrTypes;
	public TopNestedJoinExcl(AttrType[] in1, int len_in1, short[] s1_sizes,
			AttrType[] in2, int len_in2, short[] s2_sizes,
			int join_col_in1, int sortFld1Len, int join_col_in2,
			int sortFld2Len, int amt_of_mem,
			Iterator am1, Iterator am2,
			boolean in1_sorted, boolean in2_sorted,
			TupleOrder order, CondExpr[] outFilter,
			FldSpec[] proj_list, int n_out_flds, int num)
					throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, PredEvalException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception
	      {
		
		
		
		
		
		
		
		super(in1, len_in1, s1_sizes, in2, len_in2, s2_sizes, join_col_in1,
				sortFld1Len, join_col_in2, sortFld2Len, amt_of_mem, am1, am2,
				in1_sorted, in2_sorted, order, outFilter, proj_list, n_out_flds);
		System.out.println("super called");
		num1=num;
		int num2=0;
		TupleOrder o=new TupleOrder(TupleOrder.Descending);
	resAttrTypes=new AttrType[n_out_flds];
		for(int i=0;i<n_out_flds;i++)
		{
			if(proj_list[i].relation.key==0)
			{
			resAttrTypes[i]=in1[proj_list[i].offset-1];
			}
			else
			{
				resAttrTypes[i]=in2[proj_list[i].offset-1];	
			}
		}
          Tuple t=new Tuple();
//         while(num2!=1)
//         {
//		t=this.get_next();
//		t.print(resAttrTypes);
//		num2++;
//         }
	p_i1 = new Sort(this.resAttrTypes,(short)n_out_flds,this.ts_size, this, n_out_flds,
				  o,4, amt_of_mem/2);
//	while(num2!=5)
//    {
//		System.out.println("data out");
//	t=p_i1.get_next();
//	t.print(resAttrTypes);
//	num2++;
//    }
	
	System.out.println("sortmerge join done");
	
	
		
		}
	public Tuple get_TopRanked() throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception
	{	
		Tuple t;
		while((num1!=0))
		{
			num1--;
			t=p_i1.get_next();
			if(t!=null)
			{
		//  Jtuple.tupleCopy(t);
	      System.out.println("printing data");
	      t.print(resAttrTypes);
	     System.out.println(t.getScore());
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
