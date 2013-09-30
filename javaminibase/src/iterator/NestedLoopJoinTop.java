package iterator;

import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

import java.io.IOException;

import bufmgr.PageNotReadException;

import global.AttrType;
import global.TupleOrder;

public class NestedLoopJoinTop extends NestedLoopsJoins1{
	 int num1;
	 Iterator p_i1;
	public NestedLoopJoinTop(AttrType    in1[],    
			   int     len_in1,           
			   short   t1_str_sizes[],
			   AttrType    in2[],         
			   int     len_in2,           
			   short   t2_str_sizes[],   
			   int     amt_of_mem,        
			   Iterator     am1,          
			   String relationName,      
			   CondExpr outFilter[],      
			   CondExpr rightFilter[],    
			   FldSpec   proj_list[],
			   int        n_out_flds,int num)
					throws JoinNewFailed ,
					   JoinLowMemory,
					   SortException,
					   TupleUtilsException,
					   IOException, FileScanException, InvalidRelation, NestedLoopException
	      {
		
		super(in1, len_in1,t1_str_sizes, in2, len_in2, t2_str_sizes,amt_of_mem,am1,relationName,
				 outFilter,rightFilter, proj_list, n_out_flds);
		
		num1=num;TupleOrder o=new TupleOrder(1);
		
		p_i1 = new Sort(this.Jtypes,(short)n_out_flds,this.t_size, this, n_out_flds,
				  o,4, amt_of_mem);
		
		
		
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
