/**
 * 
 */
package iterator;

import global.AttrType;
import global.TupleOrder;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

import index.IndexException;

import java.io.IOException;

import bufmgr.PageNotReadException;

/**
 * @author siddu
 *
 */
public class TopNestedLoopJoinEx extends NestedLoopsJoins1 {

	/**
	 * @param in1
	 * @param len_in1
	 * @param t1_str_sizes
	 * @param in2
	 * @param len_in2
	 * @param t2_str_sizes
	 * @param amt_of_mem
	 * @param am1
	 * @param relationName
	 * @param outFilter
	 * @param rightFilter
	 * @param proj_list
	 * @param n_out_flds
	 * @throws IOException
	 * @throws NestedLoopException
	 */
	Iterator iter;
	int num1;
	AttrType[] resAttr;
	public TopNestedLoopJoinEx(AttrType[] in1, int len_in1,
			short[] t1_str_sizes, AttrType[] in2, int len_in2,
			short[] t2_str_sizes, int amt_of_mem, Iterator am1,
			String relationName, CondExpr[] outFilter, CondExpr[] rightFilter,
			FldSpec[] proj_list, int n_out_flds,int num) throws IOException,
			NestedLoopException, SortException {
		super(in1, len_in1, t1_str_sizes, in2, len_in2, t2_str_sizes,
				amt_of_mem, am1, relationName, outFilter, rightFilter,
				proj_list, n_out_flds);
		// TODO Auto-generated constructor stub
		resAttr=this.Jtypes;
		
		
		
		num1=num;
		
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
	      t.print(resAttr);
	 //    System.out.println(t.getScore());
			return t;}
			else
			{
				return null;
			}
		}
		return null;
	}

	
	
	
	
	
}
