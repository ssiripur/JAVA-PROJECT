package iterator;

import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.TupleOrder;
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
import index.IndexScan;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class TopJoinRank {

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
	 * @throws IOException 
	 * @throws HFDiskMgrException 
	 * @throws HFBufMgrException 
	 * @throws HFException 
	 * @throws InvalidTupleSizeException 
	 * @throws InvalidTypeException 
	 * @throws SpaceNotAvailableException 
	 * @throws InvalidSlotNumberException 
	 * @throws InvalidRelation 
	 * @throws TupleUtilsException 
	 * @throws FileScanException 
	 * @throws SortException 
	 * @throws FieldNumberOutOfBoundException 
	 * @throws CloneNotSupportedException 
	 */
	int numTables;
	int Strlen;
	AttrType[][] in;
	int[] len_in;
	short[][] s_sizes;
	int[] join_col_in;
	Iterator[] am;
	IndexType[] index;
	java.lang.String[] indNames;
	public Iterator it,st,sortiter;
	java.lang.String[] relNames;
	int n_buf_pgs;
	CondExpr[] outFilter;
	FldSpec[] proj_list;
	int n_out_flds;
	int num,brk;
	 private Heapfile f;
	private AttrType[] resAttrTypes,s;
	private short[] str,resstr;
	private Map<String, float[]> scoreMap;
    private int counter = 0;
	FldSpec[] proj;
	private AttrType Key_Type; 
	public TopJoinRank(int numTables, AttrType[][] in, int[] len_in,
			short[][] s_sizes, int[] join_col_in, Iterator[] am,
			IndexType[] index, String[] indNames, String[] relNames,
			int amt_of_mem, CondExpr[] outFilter, FldSpec[] proj_list,
			int n_out_flds, int num) throws HFException, HFBufMgrException, HFDiskMgrException, IOException, InvalidTypeException, InvalidTupleSizeException, InvalidSlotNumberException, SpaceNotAvailableException, FileScanException, TupleUtilsException, InvalidRelation, SortException, FieldNumberOutOfBoundException, CloneNotSupportedException {
		this.numTables = numTables;
		//projection for the score tuple
		 proj=new FldSpec[numTables];
		 proj[0]=new FldSpec(new RelSpec(RelSpec.outer),1);
		 proj[1]=new FldSpec(new RelSpec(RelSpec.outer),2);
          System.out.println("this is topjoinrank _________________________________3");
		// Copy attribute arrays.
		 Key_Type=in[0][join_col_in[0]-1];
		this.in = new AttrType[numTables][];
		for (int i = 0; i < numTables; i++) {
			AttrType[] _in = in[i];
			this.in[i] = new AttrType[_in.length];
			System.arraycopy(_in, 0, this.in[i], 0, _in.length);
		}
		//heap file
         f= new Heapfile("score.in");
		s=new AttrType[2];
		s[0]=new AttrType(AttrType.attrString);
		s[1]=new AttrType(AttrType.attrReal);
		str=new short[1];
		str[0]=100;
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
		this.brk=num;
		scoreMap = new HashMap<String, float[]>();
	while (counter < num) {

			sequential_access();

		}

		random_access();
		
		for(Object o: this.scoreMap.keySet())
		{  
	        float sco=0.0f;
			Tuple tup=new Tuple();
			tup.setHdr((short) 2, s, str);
			float[] value = this.scoreMap.get(o);
			//System.out.println("printing arrayfor key"+o);
			for(int i=0;i<numTables;i++)
			{
				//System.out.println(value[i]);
			}
		    if(value[value.length-2]==numTables)
			{
		     //System.out.println("all tables are filled"+value[value.length-2]);
			for(int k=0;k< numTables;k++)
			{
			sco += value[k];	
			}
			tup.setStrFld(1, (String)o);
			tup.setFloFld(2, sco);
			tup.print(s);
			f.insertRecord(tup.getTupleByteArray());
			}
	      }
        it=new FileScan("score.in",s,str,(short) 2,2,proj,null);
        TupleOrder o=new TupleOrder(1);
        sortiter=new Sort(s,(short)2,str,it,2,o,4,15);
	}

	public Tuple get_next() throws SortException, UnknowAttrType, LowMemException, JoinsException, IOException, Exception {
		String key=new String();
		Tuple tempTuple=new Tuple();
		Tuple TupleTuple1=new Tuple();
		
		if(((TupleTuple1=sortiter.get_next())!=null) &&(brk!=0))
		{
		brk--;
			key=(String)(TupleTuple1.getStrFld(1));
		Tuple[] tupleArray = new Tuple[this.numTables];
			for (int i = 0; i < tupleArray.length; i++) {
				tupleArray[i]=getTuple(i, key, Key_Type.attrType);  
				}

		try {
			resAttrTypes=new AttrType[n_out_flds];
			for(int i=0;i<n_out_flds;i++)
			{
				resAttrTypes[i]=in[proj_list[i].relation.key][proj_list[i].offset-1];
				
			}

			resstr=TupleUtils.setup_op_tuple(tempTuple,resAttrTypes, in, len_in, s_sizes, proj_list,n_out_flds);
				
				Projection1.Join(tupleArray,in,tempTuple, this.proj_list, this.n_out_flds);//this.sortedScoreMap.get(key)
				if(tempTuple==null)
				{
					System.out.println("no data in the tuple");
				}
				else
				{
				
				tempTuple.print(resAttrTypes);}
			System.out.println(tempTuple.getScore());
			
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		
		}
		else
		{
			tempTuple=null;
			System.out.println("print the waste data");
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

				
				if (in[i][col - 1].attrType == AttrType.attrInteger) {
					key = String.valueOf(temp.getIntFld(col));
				} else if (in[i][col - 1].attrType == AttrType.attrString) {
					key = temp.getStrFld(col);
				} else if (in[i][col - 1].attrType == AttrType.attrReal) {
					key = String.valueOf(temp.getFloFld(col));
				}
				float score = temp.getScore();

				float s_value[];
				if (scoreMap.containsKey(key))

					s_value = scoreMap.get(key);

				else {

					s_value = new float[numTables + 2];
					for (int f = 0; f < numTables; f++) {
						s_value[f] = -1;
					}

				}

			if(s_value[i]==-1)
			{
				s_value[i] =score;
				s_value[numTables] +=1;
			}
				//System.out.println(key);
			
				for(int k=0;k<numTables;k++)
				{
				//	System.out.println(s_value[k]);
				}
				
				scoreMap.put(key, s_value);

			if (s_value[numTables] >= numTables)
					counter = counter + 1;
				System.out.println("value of the counter is"+counter);

				if (counter >= num)
					break;

			} catch (Exception e) {
							e.printStackTrace();
			}
			
		}

	}

	private Tuple getTuple(int tableIndex, String key, int attrType) {
		Tuple tuple = null;
		IndexScan iscan = null;
		FldSpec[] projection = new FldSpec[len_in[tableIndex]];
		CondExpr[] expr = new CondExpr[2];
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
		try {
			tuple = iscan.get_next();
			
		} catch (Exception e) {

			e.printStackTrace();
		}

		return tuple;
	}
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
		try {
			while (iter.hasNext()) {
				skey = iter.next();
				expr[0] = new CondExpr();
				expr[0].op = new AttrOperator(AttrOperator.aopEQ);
				expr[0].type1 = new AttrType(AttrType.attrSymbol);
				expr[0].type2 = new AttrType(in[0][col - 1].attrType);
				expr[0].next = null;
				expr[1] = null;

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
				val = scoreMap.get(skey);

				for (int j = 0; j < val.length - 2; j++) {
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
						
							if((t = iscan.get_next())==null)
							{
							System.out.println("tuple is null:::::::no data");
							}
							else{
							val[j] = t.getScore();

							// updating counter .. not necessary
							val[val.length - 2] += 1;
							}
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
}