package iterator;

import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.TupleOrder;
import heap.Tuple;
import index.IndexScan;
//import interaction.Utilities;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class TopRankJoin {

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

	private Map<String, float[]> scoreMap;
	private Map<String, Float> sortedScoreMap;
	private int[] scoreField;
	private int counter = 0;

	private java.util.Iterator sortedScoreIterator;

	/**
	 * @param numTables
	 * @param in
	 * @param len_in
	 * @param s_sizes
	 * @param join_col_in
	 * @param am
	 * @param index
	 * @param indNames
	 * @param n_buf_pgs
	 * @param outFilter
	 * @param proj_list
	 * @param n_out_flds
	 * @param num
	 */
	public TopRankJoin(int numTables, AttrType[][] in, int[] len_in,
			short[][] s_sizes, int[] join_col_in, Iterator[] am,
			IndexType[] index, String[] indNames, String[] relNames,
			int amt_of_mem, CondExpr[] outFilter, FldSpec[] proj_list,
			int n_out_flds, int num) {
		this.numTables = numTables;

		// Copy attribute arrays.
		this.in = new AttrType[numTables][];
		for (int i = 0; i < numTables; i++) {
			AttrType[] _in = in[i];
			this.in[i] = new AttrType[_in.length];
			System.arraycopy(_in, 0, this.in[i], 0, _in.length);
		}

		this.len_in = len_in;
		this.s_sizes = s_sizes;
		this.join_col_in = join_col_in;

		this.am = am;
		this.index = new IndexType[index.length];
		System.arraycopy(index, 0, this.index, 0, index.length);

		this.indNames = new String[indNames.length];
		System.arraycopy(indNames, 0, this.indNames, 0, indNames.length);

		this.relNames = new String[relNames.length];
		System.arraycopy(relNames, 0, this.relNames, 0, relNames.length);

		this.n_buf_pgs = amt_of_mem;

		this.outFilter = outFilter;
		this.proj_list = proj_list;
		this.n_out_flds = n_out_flds;
		this.num = num;

		AttrType[] Jtypes = new AttrType[n_out_flds];
		short[] t_size;
		TupleOrder order = new TupleOrder(TupleOrder.Descending);

		// sorting the relation (should I pass the sorted iterator or should
		// take place in Tp Raank Join)
//		try {
//			this.am[0] = new Sort(in[0], (short) len_in[0], s_sizes[0],
//					this.am[0],n_out_flds+1, order,4, amt_of_mem , true);
//			//AttrType[] in, short len_in, short[] str_sizes, Iterator am,
//			//int sort_fld, TupleOrder sort_order, int sort_fld_len, int n_pages,
//			//boolean b
//			
//		
//			
//			this.am[1] = new Sort(in[1], (short) len_in[1], s_sizes[1],
//					this.am[1], n_out_flds+1,order,4, amt_of_mem , true);
//			//

//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

		// Hash Map to store the temporary values of sequential and random
		// access.
		scoreMap = new HashMap<String, float[]>();

		// Do sequential access till you encounter k sets.
		while (counter < num) {

			sequential_access();

		}

		random_access();

		// sorting
		Map<String, Float> tempScoreMap = new HashMap<String, Float>();

		for (Object obj : this.scoreMap.keySet()) {
			float[] value = this.scoreMap.get(obj);
			tempScoreMap.put((String) obj, value[numTables + 1]);
		}

		ScoreComparator<String> comparator = new ScoreComparator<String>(
				tempScoreMap);
		this.sortedScoreMap = new TreeMap<String, Float>(comparator);

		this.sortedScoreMap.putAll(tempScoreMap);

		this.sortedScoreIterator = this.sortedScoreMap.keySet().iterator();

		// projecting
		Tuple jTuple = new Tuple();
		short[] strSizes = null;
		this.resAttrTypes = new AttrType[proj_list.length];
		try {
			this.str_sizes = TupleUtils.setup_op_tuple(jTuple,new AttrType[n_out_flds], in, s_sizes, proj_list,n_out_flds);
//			Tuple op_t, AttrType[] op_attrs,
//			AttrType[][] in_t_attrs, short[][] in_t_str_sizes,
//			FldSpec[] proj_list,int nOutFlds
			this.joinTuple = new Tuple(jTuple.getLength());
			this.joinTuple.setHdr((short) n_out_flds, this.resAttrTypes,
					this.str_sizes);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Fetch the records and join them.

	}

	public Tuple get_next() {
		float scorefld;
		Tuple tempTuple=null;

		if (this.sortedScoreIterator.hasNext()) {

			tempTuple=new Tuple(this.joinTuple);

			// Create tuples for the tables.
			Tuple[] tupleArray = new Tuple[this.numTables];
			String key=(String) this.sortedScoreIterator.next();
			for (int i = 0; i < tupleArray.length; i++) {
				tupleArray[i]=getTuple(i, key, AttrType.attrInteger);
				//scorefld=getScore
			}
			
			float score=1.0f;
			if(this.scoreMap.containsKey(key)){
				float[] arr=this.scoreMap.get(key);
				score=arr[numTables+1];
			}
						
			try {
				//join
				//Projection.Join(tupleArray,in,tempTuple, this.proj_list, this.n_out_flds);//this.sortedScoreMap.gaet(key)
			Projection1.Join(tupleArray, in, tempTuple, this.proj_list, this.n_out_flds);
			float sco;
			for(int j=0;j<numTables;j++)
			{//sco=sco+s_value[j];
			
			}
			tempTuple.setScore(score);
			//tempTuple.setScore(f);
				//Join( Tuple  t1, AttrType type1[],
                  //      Tuple  t2, AttrType type2[],
     	           //Tuple Jtuple, FldSpec  perm_mat[], 
                     //   int nOutFlds
                        //public static void Join(Tuple[] in_tuples, AttrType[][] in_attrs,
            			////Tuple out_tuple, FldSpec[] proj_list)
			tempTuple.print(resAttrTypes);
                        
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		}

		return tempTuple;
	}

	public int num_scanned(int in_rel) {

		return 0;
	}

	public int num_probed(int in_rel) {
		return 0;
	}

	public void close() {

	}

	/**
	 * Performs sequential access across all tables based on the field to be
	 * joined
	 */
	private void sequential_access() {
		Tuple temp;
		int col;
		String key = null;

		for (int i = 0; i < numTables; i++) {
			try {
				temp = am[i].get_next();
				col = join_col_in[i];

				// get the key value for the map(i.e) the field on which join is
				// performed
				if (in[i][col - 1].attrType == AttrType.attrInteger) {
					key = String.valueOf(temp.getIntFld(col));
				} else if (in[i][col - 1].attrType == AttrType.attrString) {
					key = String.valueOf(temp.getCharFld(col));
				} else if (in[i][col - 1].attrType == AttrType.attrReal) {
					key = String.valueOf(temp.getFloFld(col));
				}

				// get the score
				float score = temp.getScore();

				float s_value[];

				// get the key and its value if exist from hashmap
				if (scoreMap.containsKey(key))

					s_value = scoreMap.get(key);

				else {
					// assign a float array and initialize the score section to
					// -1 : it'll be easy to find out if the score is filled

					s_value = new float[numTables + 2];
					for (int f = 0; f < numTables; f++) {
						s_value[f] = -1;
					}

				}

				// update the score
				s_value[i] = score;
				s_value[numTables] += 1;

				// add the key in the map
				scoreMap.put(key, s_value);

				// if all the scores are filled for the key, increment the
				// counter
				if (s_value[numTables] >= numTables)
					counter = counter + 1;

				if (counter >= num)
					break;

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private Tuple getTuple(int tableIndex, String key, int attrType) {
		Tuple tuple = null;
		IndexScan iscan = null;
		FldSpec[] projection = new FldSpec[len_in[tableIndex]];
		CondExpr[] expr = new CondExpr[2];

		// setting the conditional exp
		expr[0] = new CondExpr();
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(attrType);
		expr[0].next = null;
		expr[1] = null;

		int ikey;
		float rkey;

		if (attrType == AttrType.attrInteger) {
			ikey = Integer.parseInt(key);
			expr[0].operand2.integer = ikey;
		}

		else if (attrType == AttrType.attrReal) {
			rkey = Float.parseFloat(key);
			expr[0].operand2.real = rkey;

		} else {
			expr[0].operand2.string = key;
		}

		for (int i = 0; i < len_in[tableIndex]; i++) {
			projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);			
		}

		// fix the field on which to be indexed
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
				join_col_in[tableIndex]);

		try {
			iscan = new IndexScan(index[tableIndex], relNames[tableIndex], indNames[tableIndex], in[tableIndex],
					s_sizes[tableIndex], len_in[tableIndex], len_in[tableIndex], projection, expr, join_col_in[tableIndex],
					false);
		} catch (Exception e) {

			e.printStackTrace();
		}

		// may have to consider for duplicates...
		try {
			tuple = iscan.get_next();
		} catch (Exception e) {

			e.printStackTrace();
		}

		return tuple;
	}

	/**
	 * perform random access and update all the scores in hashmap
	 */
	private void random_access() {

		Set<String> keys = scoreMap.keySet();
		java.util.Iterator<String> iter = keys.iterator();
		int col = join_col_in[0];
		String skey;
		int ikey;
		float rkey;
		float val[];
		IndexScan iscan = null;
		FldSpec[] projection = new FldSpec[1];
		CondExpr[] expr = new CondExpr[2];

		// loop through each value and fill the scores
		try {
			while (iter.hasNext()) {

				// getting the key
				skey = iter.next();

				// setting the conditional exp
				expr[0] = new CondExpr();
				expr[0].op = new AttrOperator(AttrOperator.aopEQ);
				expr[0].type1 = new AttrType(AttrType.attrSymbol);
				expr[0].type2 = new AttrType(in[0][col - 1].attrType);
				expr[0].next = null;
				expr[1] = null;

				// find the type of attribute type of the key to set to the
				// conditional exp

				if (in[0][col - 1].attrType == AttrType.attrInteger) {
					ikey = Integer.parseInt(skey);
					expr[0].operand2.integer = ikey;
				}

				else if (in[0][col - 1].attrType == AttrType.attrReal) {
					rkey = Float.parseFloat(skey);
					expr[0].operand2.real = rkey;

				} else {
					expr[0].operand2.string = skey;
				}

				// get the value of the key
				val = scoreMap.get(skey);

				for (int j = 0; j < val.length - 2; j++) {
					// if the score is not updated, perform random access
					if (val[j] == -1) {
						// fix the field to be projected while indexing, really
						// doesn't matter
						projection[0] = new FldSpec(new RelSpec(RelSpec.outer),
								join_col_in[j]);

						// fix the field on which to be indexed
						expr[0].operand1.symbol = new FldSpec(new RelSpec(
								RelSpec.outer), join_col_in[j]);

						try {
							iscan = new IndexScan(index[j], relNames[j],
									indNames[j], in[j], s_sizes[j], len_in[j],
									1, projection, expr, join_col_in[j], false);
						} catch (Exception e) {

							e.printStackTrace();
						}

						Tuple t = null;

						// may have to consider for duplicates...
						try {
							t = iscan.get_next();
							val[j] = t.getScore();

							// updating counter .. not necessary
							val[val.length - 2] += 1;

						} catch (Exception e) {

							e.printStackTrace();
						}

					}
					// adding the score
					val[val.length - 1] += val[j];
				}
				// updating with average
				val[val.length - 1] = val[val.length - 1] / numTables;
				scoreMap.put(skey, val);
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
	}

	/**
	 * printing hash map
	 */
	private void printMap() {
		Set<String> keys = scoreMap.keySet();
		java.util.Iterator<String> iter = keys.iterator();
		String key;
		float[] value;
		try {
			while (iter.hasNext()) {
				key = iter.next();
				value = scoreMap.get(key);
				System.out.println("the key is" + key);
				for (int i = 0; i < value.length; i++) {
					System.out.println("the value is" + value[i]);
				}

			}
		} catch (Exception e) {

		}
	}

}
