package iterator;

import global.AttrType;
import global.IndexType;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

import java.io.IOException;

import bufmgr.PageNotReadException;

public class Filescan2 extends Iterator {

	/**
	 * @param args
	 */
	public Filescan2(int numTables, AttrType[][] in, int[] len_in,
			short[][] s_sizes, int[] join_col_in, Iterator[] am,
			IndexType[] index, String[] indNames, String[] relNames,
			int amt_of_mem, CondExpr[] outFilter, FldSpec[] proj_list,
			int n_out_flds, int num)
	{
		
		System.out.println(join_col_in.length);
		System.out.println(am.length);
		System.out.println(index.length);
		System.out.println(indNames.length);
		System.out.println(relNames.length);
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException,
			InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, TupleUtilsException, PredEvalException,
			SortException, LowMemException, UnknowAttrType,
			UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws IOException, JoinsException, SortException,
			IndexException {
		// TODO Auto-generated method stub
		
	}

}
