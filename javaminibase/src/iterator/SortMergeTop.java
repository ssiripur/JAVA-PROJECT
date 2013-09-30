package iterator;

import global.AttrType;
import global.TupleOrder;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

import java.io.IOException;

import bufmgr.PageNotReadException;

public class SortMergeTop extends SortMerge1{
	Iterator p_i1;
	int num1;
	
	public SortMergeTop(AttrType[] in1, int len_in1, short[] s1_sizes,
			AttrType[] in2, int len_in2, short[] s2_sizes,
			int join_col_in1, int sortFld1Len, int join_col_in2,
			int sortFld2Len, int amt_of_mem,
			Iterator am1, Iterator am2,
			boolean in1_sorted, boolean in2_sorted,
			TupleOrder order, CondExpr[] outFilter,
			FldSpec[] proj_list, int n_out_flds, int num)
					throws JoinNewFailed ,
					   JoinLowMemory,
					   SortException,
					   TupleUtilsException,
					   IOException, FileScanException, InvalidRelation
	      {
		
		super(in1, len_in1, s1_sizes, in2, len_in2, s2_sizes, join_col_in1,
				sortFld1Len, join_col_in2, sortFld2Len, amt_of_mem, am1, am2,
				in1_sorted, in2_sorted, order, outFilter, proj_list, n_out_flds);
		num1=num;
		/*SortMerge sm= new SortMerge(in1, len_in1, s1_sizes, in2, len_in2, s2_sizes, join_col_in1,
				sortFld1Len, join_col_in2, sortFld2Len, amt_of_mem, am1, am2,
				in1_sorted, in2_sorted, order, outFilter, proj_list, n_out_flds
				   );*/
		TupleOrder o=new TupleOrder(1);
		/*p_i1 = new Sort(sm.Jtypes,(short)n_out_flds,sm.ts_size, sm, n_out_flds+1,
				  o,4, amt_of_mem / 2);*/
		p_i1 = new Sort(this.Jtypes,(short)n_out_flds,this.ts_size, this, n_out_flds,
				  o,4, amt_of_mem ,true);
		
		
		
	      }
	public Tuple get_TopRanked() throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception
	{	
		
		while(num1!=0)
		{
			num1--;
			return p_i1.get_next();
			
			
		}
		return null;
	}
	
	
}
