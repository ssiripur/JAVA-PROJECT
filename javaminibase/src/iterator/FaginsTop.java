/**
 * 
 */
package iterator;

import global.AttrType;
import global.IndexType;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;

import java.io.IOException;

import bufmgr.PageNotReadException;

/**
 * @author beck
 *
 */
public class FaginsTop extends Iterator {

	/**
	 * 
	 */
	int numTables;
	AttrType[][] in;
	int[] len_in;
	short[][] s_sizes;
	int[] join_col_in;
	Iterator[] am;
	IndexType[] index;
	java.lang.String[] indNames;

	// make sure if this should be included
	java.lang.String[] relNames;
	int n_buf_pgs;
	CondExpr[] outFilter;
	FldSpec[] proj_list;
	int n_out_flds;
	int num;

	private Tuple joinTuple;
	private AttrType[] resAttrTypes;
	private short[] str_sizes;
			
	public FaginsTop(int numTables, AttrType[][] in, int[] len_in,
			short[][] s_sizes, int[] join_col_in, Iterator[] am,
			IndexType[] index, String[] indNames, String[] relNames,
			int amt_of_mem, CondExpr[] outFilter, FldSpec[] proj_list,
			int n_out_flds, int num) {
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see iterator.Iterator#get_next()
	 */
	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException,
			InvalidTupleSizeException, InvalidTypeException,
			PageNotReadException, TupleUtilsException, PredEvalException,
			SortException, LowMemException, UnknowAttrType,
			UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see iterator.Iterator#close()
	 */
	@Override
	public void close() throws IOException, JoinsException, SortException,
			IndexException {
		// TODO Auto-generated method stub

	}

}
