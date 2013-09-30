package iterator;

import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.RID;
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
import index.IndexException;
import index.IndexScan;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import bufmgr.PageNotReadException;

public class ThresoldTopK extends Iterator {
	int numTables;
	int Strlen;
	AttrType[][] in;
	int[] len_in;
	short[][] s_sizes;
	int[] join_col_in;
	Iterator[] am;
	IndexType[] index;
	java.lang.String[] indNames;
	public Iterator it, st, sortiter;
	java.lang.String[] relNames;
	int n_buf_pgs;
	CondExpr[] outFilter;
	FldSpec[] proj_list;
	int n_out_flds;
	int num, brk;
	//private Heapfile f;
	private Heapfile f1;
	Sort sortiter1;
	private AttrType[] resAttrTypes, s;
	private short[] str, resstr;
	private Set<String> scored;
	//private ArrayList<Float> scorear = new ArrayList<Float>();
	private int counter = 0;
	FldSpec[] proj1;
	private AttrType Key_Type;
	AttrType[] strTypes;
	float[] current;
	float finalcurrent = numTables;
	float[] finalscoresk;
	FldSpec[][] project2;
	FldSpec[] project3;
	AttrType[] sumAttr;
    int sumlen=1;
    short[] heapstr;
     int finalscorec=0;
	public ThresoldTopK(int numTables, AttrType[][] in, int[] len_in,
			short[][] s_sizes, int[] join_col_in, Iterator[] am,
			IndexType[] index, String[] indNames, String[] relNames,
			int amt_of_mem, CondExpr[] outFilter, FldSpec[] proj_list,
	
			int n_out_flds, int num) throws UnknowAttrType, LowMemException,
			JoinsException, Exception {
		this.numTables = numTables;
     
		
		proj1 = new FldSpec[numTables + 2];
		for (int i = 0; i < numTables + 2; i++) {
			proj1[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
		}
		finalscoresk=new float[num+1];
		
		
		
		System.out.println("this is thresold" + "TopK -----------------");

		Key_Type = in[0][join_col_in[0] - 1];
		this.in = new AttrType[numTables][];
		for (int i = 0; i < numTables; i++) {
			AttrType[] _in = in[i];
			this.in[i] = new AttrType[_in.length];
			System.arraycopy(_in, 0, this.in[i], 0, _in.length);
		}
		f1 = new Heapfile("scored.in");
		strTypes = new AttrType[numTables + 2];
		strTypes[0] = new AttrType(AttrType.attrString);

		for (int i = 1; i < numTables + 2; i++) {
			strTypes[i] = new AttrType(AttrType.attrReal);
		}
		project2=new FldSpec[numTables][];
          for(int i=0;i<numTables;i++)
          {
        	  project2[i]=new FldSpec[len_in[i]];
        	  for(int j=0;j<len_in[i];j++)
        	  {
        		  project2[i][j]=new FldSpec(new RelSpec(RelSpec.outer),j+1);
        	  }
          }
         
		str = new short[1];
		str[0] = 100;
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
		this.brk = num;
		scored = new HashSet<String>();
		current = new float[numTables];
		int atcount=0;
		
		   for(int k:len_in)
	        {
	        	sumlen+=k;
	        }
		   sumAttr=new AttrType[sumlen];
		   for(AttrType[] k:in)
		   {
			   for(AttrType p:k)
			   {
				  
				   sumAttr[atcount]=p;
				   atcount++;
			   }
		   }
		   sumAttr[atcount]=new AttrType(AttrType.attrReal);
		   project3=new FldSpec[sumlen];
	          for(int i=0;i<sumlen;i++)
	          {
	        	  project3[i]=new FldSpec(new RelSpec(RelSpec.outer),i+1);
	          }
		   atcount=0;
		   short strlen=0;
		   for(short[] l:s_sizes)
		   {
			   strlen+=l.length;
		   }
		    heapstr=new short[strlen];
		   //System.out.println("length of strings in arris");
		   for(short[] l:s_sizes)
		   {
			  for(short p:l)
			  {
				  
				  heapstr[atcount]=p;
				  atcount++;
				  //System.out.println(p);
			  }
		   }
		  
		   
		for (int l = 0; l < numTables; l++) {
			current[l] = 1;
		}
		float thresold = numTables;
		int counter = 0;
		finalcurrent=numTables;
		for(float j:finalscoresk)
		{
			j=0.0f;
		}
		//finalcurrent >finalscoresk[finalscoresk.length-1]
		int count=0;
		while (finalcurrent>finalscoresk[1]) {
			count++;
			System.out.println("printing out of the data");
			Thresold_access();
			System.out.println("iteration done------------");
			System.out.println("current array is:");
			finalcurrent = 0;
			System.out.println("score");
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();

			for (float k : current) {
				finalcurrent += k;
				System.out.print(k + ",");
			}

			System.out.println("final current value is:" + finalcurrent);
			int counte = 0;
			System.out.println();System.out.println();System.out.println();System.out.println();System.out.println();System.out.println();System.out.println();System.out.println();System.out.println();
			


		}
		System.out.println("break condition reached++++++++++++hurray!!!!!!!!!");
		


//		Tuple tup = new Tuple();

		// for(Object o: this.scored.keySet())
		// {
		// float sco=0.0f;
		//
		// tup.setHdr((short) (numTables+2), strTypes, str);
		// ArrayList<float[]> k = this.scored.get(o);
		// float[] value;
		// for(int j=0;j<k.size();j++)
		// {
		// value=k.get(j);
		// sco=0.0f;
		// if(value[value.length-2]==numTables)
		// {
		// tup.setStrFld(1, (String)o);
		// for(int l=0;l< numTables;l++)
		// {
		// sco += value[l];
		// tup.setFloFld(l+2,value[l]);
		// }
		// tup.setFloFld(numTables+2, sco);
		// tup.print(strTypes);
		// f1.insertRecord(tup.getTupleByteArray());
		// }
		//
		// }}
		FileScan it1=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,project3,null);
//		Iterator it1 = new FileScan("scored.in", strTypes, str,
//				(short) (numTables + 2), (numTables + 2), proj1, null);
//		public FileScan(String file_name, AttrType in1[], short s1_sizes[],
//				short len_in1, int n_out_flds, FldSpec[] proj_list,
//				CondExpr[] outFilter) 
		
		
		
		
		
		System.out.println("pruning the tuples for the sort");
		Tuple tu=new Tuple();
		tu.setHdr((short)sumlen, sumAttr, heapstr);
		int count1=0;
		int prunned=0;
		while((tu=it1.get_next())!=null)
		{
			RID r=new RID();
			r=it1.getrid();
			
			float score=tu.getFloFld(sumlen);		
			if(score<finalcurrent)
			{
				//System.out.println("tuple to be pruned is");
				//tu.print(sumAttr);
				f1.deleteRecord(r);
				prunned++;
			}
			else
			{
				count1++;
			}
			
		}
		System.out.println("TOtal no of prunned data is:"+prunned);
		System.out.println("TOtal no of filled data is:"+count1);
		FileScan it2=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,project3,null);
		sortiter1 = new Sort(sumAttr, (short) (sumlen), heapstr, it2,
				(short)sumlen, new TupleOrder(TupleOrder.Descending), 4, amt_of_mem);
		
		//Iteration .......
//		sortiter1 = new Sort(strTypes, (short) (numTables + 2), str, it1,
//				(numTables + 2), new TupleOrder(TupleOrder.Descending), 4, 15);
//iteration.....
		
		// while((tup=sortiter1.get_next())!=null)
		// {
		// tup.print(strTypes);
		// }//working code............
	System.out.println("output    of     the  data would be");

	}

	private int Thresold_access() {
		Tuple temp;
		int col;
		String key = null;
		boolean flag = true;
		for (int j = 0; j < numTables; j++) {
			System.out.println("Iteration from table:" + j);
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();

			try {
				// temp=am[j].get_next();
				col = join_col_in[j];
				float score = 0;

				temp = am[j].get_next();
				col = join_col_in[j];

				if (in[j][col - 1].attrType == AttrType.attrInteger) {
					key = String.valueOf(temp.getIntFld(col));
				} else if (in[j][col - 1].attrType == AttrType.attrString) {
					key = temp.getStrFld(col);
				} else if (in[j][col - 1].attrType == AttrType.attrReal) {
					key = String.valueOf(temp.getFloFld(col));
				}
				score = temp.getScore();
				current[j] = score;
				if (scored.contains(key)) {
					System.out.println("key is already in scored:" + key);
					System.out.println("with score" + temp.getScore());
				} else {
					scored.add(key);
					System.out.println("------------" + "from table" + j
							+ "key selected is" + key);
					System.out.println("score is :" + temp.getScore());
					Thresold_key_By_access(key);
					
					// ArrayList<float[]> al;
					// Set<String> s=scored.keySet();
					// java.util.Iterator<String> iter= s.iterator();
					// String keys;
					// int count=0;
					// while(iter.hasNext())
					// {
					//
					// keys=iter.next();
					// System.out.println("keys recieved is "+keys);
					// al=scored.get(keys);
					// int finalscore=0;
					//
					// for(float[] k:al)
					// {
					// finalscore=0;
					// for(int l=0;l<numTables;l++)
					// finalscore+=k[l];
					// k[numTables]=finalscore;
					// if(finalscore>finalcurrent)
					// {
					// count++;
					// }
					//
					// }
					// if(count>=num)
					// {
					// return -1;
					// }
					// else
					// return 0;
					// }
					//

				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		return 0;
	}

	private void Thresold_key_By_access(String key) throws IndexException,
			UnknownKeyTypeException, IOException,
			FieldNumberOutOfBoundException, InvalidTypeException,
			InvalidTupleSizeException, InvalidSlotNumberException,
			SpaceNotAvailableException, HFException, HFBufMgrException,
			HFDiskMgrException {
		float rkey;
		
		IndexScan iscan = null;
		int col = join_col_in[0];
		int ikey;
        TupleStore[] Tuples=new TupleStore[numTables];
        for(int i=0;i<numTables;i++)
        {
        	Tuples[i]=new TupleStore();
        }
		System.out.println("key received by Thresold_key_By_access is:" + key);
		ArrayList<float[]> al = new ArrayList<float[]>();
		FldSpec[] projection = new FldSpec[1];
		CondExpr[] expr = new CondExpr[2];
		expr[0] = new CondExpr();
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(in[0][col - 1].attrType);
		expr[0].next = null;
		expr[1] = null;// while((tup=sortiter1.get_next())!=null)// while((tup=sortiter1.get_next())!=null)
		// {
		// tup.print(strTypes);
		// }//working code............
		// {
		// tup.print(strTypes);
		// }//working code............
		if (in[0][col - 1].attrType == AttrType.attrInteger) {
			ikey = Integer.parseInt(key);
			expr[0].operand2.integer = ikey;
		}

		else if (in[0][col - 1].attrType == AttrType.attrReal) {
			rkey = Float.parseFloat(key);
			expr[0].operand2.real = rkey;

		} else {
			expr[0].operand2.string = key;
		}
		for (int i = 0; i < numTables; i++) {
			projection[0] = new FldSpec(new RelSpec(RelSpec.outer),
					join_col_in[i]);

			expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
					join_col_in[i]);
			int count = 0;
			try {

//				iscan = new IndexScan(index[i], relNames[i], indNames[i],
//						in[i], s_sizes[i], len_in[i], 1, projection, expr,
//						join_col_in[i], false);
				iscan = new IndexScan(index[i], relNames[i], indNames[i],
					in[i], s_sizes[i], len_in[i],len_in[i] , project2[i], expr,
						join_col_in[i], false);
			} catch (Exception e) {

				e.printStackTrace();
			}

			Tuple tup;
			if (i == 0) {
				System.out.println("from the table:"+i);
				while ((tup = iscan.get_next()) != null) {
				//	System.out.println("adding data at index"+i);
					tup.print(in[i]);
					Tuple t1=new Tuple();
					t1.setHdr((short)len_in[i], in[i], s_sizes[i]);
					t1.tupleCopy(tup);
					
					Tuples[i].n.add(t1);
					float score = tup.getScore();
					float[] newarr = new float[numTables + 1];
					newarr[i] = score;
					al.add(newarr);}

			} else {

				int k = al.size();
				float score = 0;
				System.out.println("from the table:"+i);
				while ((tup = iscan.get_next()) != null) {
					Tuple t1=new Tuple(tup.size());
					t1.setHdr((short)len_in[i], in[i], s_sizes[i]);
					t1.tupleCopy(tup);
                     Tuples[i].n.add(t1);
                  //   System.out.println("adding data in the index "+i);
                     tup.print(in[i]);
					count++;
					for (int j = 0; j < k; j++) {
						score = tup.getScore();
						if (count == 1) {
							float[] newarr = al.get(j);
							newarr[i] = score;
						} else {

							float[] newarr = new float[numTables + 1];
							System.arraycopy(al.get(j), 0, newarr, 0,
									numTables + 1);
							newarr[i] = score;
							al.add(newarr);
						}

					}
				}//end while
				}//end else
				
				for(int k1=0;k1<numTables;k1++)
				{
					System.out.println("table:hello table no is"+k1);
				for(int j1=0;j1<Tuples[k1].n.size();j1++)
				{
				Tuple tu=Tuples[k1].n.get(j1);
				//tu.print(in[k1]);
				}
				}
//				Tuple Tu=new Tuple ();
//			Tu.setHdr((short)sumlen, sumAttr, heapstr);
//			for(int j=0;j<sumlen;j++)
//				{
//				if(sumAttr[j].attrType==AttrType.attrInteger)
//				{
//					
//					
//					Tu.setIntFld(j+1, -1);
//				}
//				if(sumAttr[j].attrType==AttrType.attrReal)
//				{
//					
//					Tu.setFloFld(j+1, -1.0f);
//				}
//				if(sumAttr[j].attrType==AttrType.attrString)
//				{
//					
//					Tu.setStrFld(j+1, "val");
//				}
//				}
			
			//Tu.print(sumAttr);
//			for(int j=0;j<Tuples[0].n.size();j++)
//			{
//				Tuple tu=Tuples[0].n.get(j);
//				tu.print(in[j]);
//			}
//			for(int j=0;j<Tuples[1].n.size();j++)
//			{
//				Tuple tu=Tuples[1].n.get(j);
//				tu.print(in[j]);
//			}
			
		//	ArrayList<Tuple[]> finalTuples=new ArrayList<Tuple[]>();
			int size=0;
			////////////////////////////////////////////////////////////////////////////new approach
			Tuple finalTuple=new Tuple();
			finalTuple.setHdr((short)sumlen, sumAttr,heapstr);
			if(numTables==2)
			{
				float sum=0;
			Tuple[] ar=new Tuple[numTables];
			for(int l=0;l<Tuples[0].n.size();l++)
			{
			ar[0]=Tuples[0].n.get(l)	;
			System.out.println("printing the new value:");
			ar[0].print(in[0]);
			ar[0].setHdr((short)len_in[0], in[0], heapstr);
			float tscore=ar[0].getFloFld(len_in[0]);
			sum+=ar[0].getFloFld(len_in[0]);
			for(int p=1;p<len_in[0]+1;p++)
			{
				if(sumAttr[p-1].attrType==AttrType.attrInteger)
				{
					
					int c =ar[0].getIntFld(p);
				//	System.out.println("printing int field:"+c);
					finalTuple.setIntFld(p, c);
				}
				if(sumAttr[p-1].attrType==AttrType.attrReal)
				{
					float c =ar[0].getFloFld(p);
					//System.out.println("printing float field:"+c);
					finalTuple.setFloFld(p, c);
				}
				if(sumAttr[p-1].attrType==AttrType.attrString)
				{
					String c =ar[0].getStrFld(p);
					//System.out.println("printing String field:"+c);
					finalTuple.setStrFld(p, c);
				}
				
			}
		//	sum+=ar[0].getFloFld(len_in[0]);
			
				for(int m=0;m<Tuples[1].n.size();m++)
				{
					ar[1]=Tuples[1].n.get(m);
					for(int p=1;p<len_in[1]+1;p++)
					{
						if(sumAttr[p-1+len_in[0]].attrType==AttrType.attrInteger)
						{
							
							int c =ar[1].getIntFld(p);
						//	System.out.println("printing int field:"+c);
							finalTuple.setIntFld(p+len_in[0], c);
						}
						if(sumAttr[p-1+len_in[0]].attrType==AttrType.attrReal)
						{
							float c =ar[1].getFloFld(p);
							//System.out.println("printing float field:"+c);
							finalTuple.setFloFld(p+len_in[0], c);
						}
						if(sumAttr[p-1+len_in[0]].attrType==AttrType.attrString)
						{
							String c =ar[1].getStrFld(p);
							//System.out.println("printing String field:"+c);
							finalTuple.setStrFld(p+len_in[0], c);
						}
						
					}
					float score=ar[1].getFloFld(len_in[1]);
					sum+=ar[1].getFloFld(len_in[1]);
                    finalTuple.setFloFld(sumlen, sum);
					finalTuple.print(sumAttr);
					f1.insertRecord(finalTuple.getTupleByteArray());
					///need to add the data.....
					
					finalscoresk[0]=sum;
					
					boolean flag=true;
					if(flag)
					{
						
					ArrayList<Float> arra=new ArrayList<Float>();
					for(float kf:finalscoresk)
					{
					arra.add(kf);
					}
					Collections.sort(arra);
				//	Collections.so
					for(int ke=0;ke<arra.size();ke++)
					{
						finalscoresk[ke]=arra.get(ke);
					}
					flag=false;}
					
					
					sum-=score;
			    }
				sum-=tscore;
			}
				
			}
			
//			if(numTables==2)
//			{
//			Tuple[] ar=new Tuple[numTables];
//			for(int l=0;l<Tuples[0].n.size();l++)
//			{
//			ar[0]=Tuples[0].n.get(l)	;
//			
//				for(int m=0;m<Tuples[1].n.size();m++)
//				{	
//					ar[1]=Tuples[1].n.get(m);
//					finalTuples.add(ar);
//			    }
//			}
//				
//			}
			if(numTables==3)
			{
				//Tuple[] ar=new Tuple[numTables];
				
				float sum=0;
				Tuple[] ar=new Tuple[numTables];	
				
				for(int l=0;l<Tuples[0].n.size();l++)
				{
				//ar[0]=Tuples[0].n.get(l)	;
				
				
				//added for 3
				ar[0]=Tuples[0].n.get(l)	;
				System.out.println("printing the new value:");
				ar[0].print(in[0]);
				ar[0].setHdr((short)len_in[0], in[0], heapstr);
				float tscore=ar[0].getFloFld(len_in[0]);
				sum+=tscore;
				for(int p=1;p<len_in[0]+1;p++)
				{
					if(sumAttr[p-1].attrType==AttrType.attrInteger)
					{
						
						int c =ar[0].getIntFld(p);
					//	System.out.println("printing int field:"+c);
						finalTuple.setIntFld(p, c);
					}
					if(sumAttr[p-1].attrType==AttrType.attrReal)
					{
						float c =ar[0].getFloFld(p);
						//System.out.println("printing float field:"+c);
						finalTuple.setFloFld(p, c);
					}
					if(sumAttr[p-1].attrType==AttrType.attrString)
					{
						String c =ar[0].getStrFld(p);
						//System.out.println("printing String field:"+c);
						finalTuple.setStrFld(p, c);
					}
					
				}
				//added for the value
					for(int m=0;m<Tuples[1].n.size();m++)
					{	
						
						//ar[1]=Tuples[1].n.get(m);
						
						ar[1]=Tuples[1].n.get(m);
						for(int p=1;p<len_in[1]+1;p++)
						{
							if(sumAttr[p-1+len_in[0]].attrType==AttrType.attrInteger)
							{
								
								int c =ar[1].getIntFld(p);
							//	System.out.println("printing int field:"+c);
								finalTuple.setIntFld(p+len_in[0], c);
							}
							if(sumAttr[p-1+len_in[0]].attrType==AttrType.attrReal)
							{
								float c =ar[1].getFloFld(p);
								//System.out.println("printing float field:"+c);
								finalTuple.setFloFld(p+len_in[0], c);
							}
							if(sumAttr[p-1+len_in[0]].attrType==AttrType.attrString)
							{
								String c =ar[1].getStrFld(p);
								//System.out.println("printing String field:"+c);
								finalTuple.setStrFld(p+len_in[0], c);
							}
							
						}
						float score=ar[1].getFloFld(len_in[1]);
						sum+=ar[1].getFloFld(len_in[1]);
						for(int n=0;n<Tuples[2].n.size();n++)
						{
							ar[2]=Tuples[2].n.get(n);
							for(int p=1;p<len_in[2]+1;p++)
							{
								if(sumAttr[p-1+len_in[0]+len_in[1]].attrType==AttrType.attrInteger)
								{
									
									int c =ar[2].getIntFld(p);
								//	System.out.println("printing int field:"+c);
									finalTuple.setIntFld(p+len_in[0]+len_in[1], c);
								}
								if(sumAttr[p-1+len_in[0]+len_in[1]].attrType==AttrType.attrReal)
								{
									float c =ar[2].getFloFld(p);
									//System.out.println("printing float field:"+c);
									finalTuple.setFloFld(p+len_in[0]+len_in[1], c);
								}
								if(sumAttr[p-1+len_in[0]+len_in[1]].attrType==AttrType.attrString)
								{
									String c =ar[2].getStrFld(p);
									//System.out.println("printing String field:"+c);
									finalTuple.setStrFld(p+len_in[0]+len_in[1], c);
								}
								
							}
							float score3=ar[2].getFloFld(len_in[2]);
							sum+=score3;
		                    finalTuple.setFloFld(sumlen, sum);
						//	finalTuple.print(sumAttr);
							f1.insertRecord(finalTuple.getTupleByteArray());
							///need to add the data.....
							
							finalscoresk[0]=sum;
							
							boolean flag=true;
							if(flag)
							{
								
							ArrayList<Float> arra=new ArrayList<Float>();
							for(float kf:finalscoresk)
							{
							arra.add(kf);
							}
							Collections.sort(arra);
						//	Collections.so
							for(int ke=0;ke<arra.size();ke++)
							{
								finalscoresk[ke]=arra.get(ke);
							}
							flag=false;
							}
							
						  sum-=score3;	
				//		finalTuples.add(ar);
						}
						sum-=score;
				    }
					sum-=tscore;
				}
				
			}
			if(numTables==4)
			{
				Tuple[] ar=new Tuple[numTables];
				for(int l=0;l<Tuples[0].n.size();l++)
				{
				ar[0]=Tuples[0].n.get(l)	;
				
					for(int m=0;m<Tuples[1].n.size();m++)
					{	
						
						ar[1]=Tuples[1].n.get(m);
						for(int n=0;n<Tuples[2].n.size();n++)
						{
							ar[2]=Tuples[2].n.get(n);
							for(int o=0;o<Tuples[3].n.size();o++)
							{ar[3]=Tuples[3].n.get(n);
			//			finalTuples.add(ar);
						}
				    }
				}
				
			}
			
			
			
			
			
			
			
			
			
			///////////////////////////////////////////////////////////////////////////
//			
//			for(int j1=0;j1<Tuples[0].n.size();j1++)
//			{
//				Tuple[] ar=new Tuple[numTables];
//				ar[0]=Tuples[0].n.get(j1);
//				finalTuples.add(ar);
//			
//			}
//			for(int j1=1;j1<numTables;j1++)
//			{
//				//int array_size=Tuples[j1-1].n.size();
//				int array_size=finalTuples.size();
//				for(int k1=0;k1<Tuples[j1].n.size();k1++)
//				{
//					Tuple tu=Tuples[j1].n.get(k1);
//					System.out.println("tuple selectyed is:");
//					tu.print(in[j1]);
//					
//					for(int l1=0;l1<array_size;l1++)
//					{
//						Tuple[] ar=finalTuples.get(l1);
//						ar[j1]=tu;
//						finalTuples.add(ar);
//						//System.arraycopy(src, srcPos, dest, destPos, length)
//					}
//				}
////				for(Tuple[] ar:finalTuples)
////				{
////					System.out.println("new tuple");
////					ar[0].print(in[0]);
////					ar[1].print(in[1]);
////				}
//				for(int k2=0;k2<array_size;k2++)
//				{
//					finalTuples.remove(0);
//				}
//			}
				/////////////////////////////////////////////////////////////////////////////////////////old imple
				
				
				
				
				
				//################################old
//			for(Tuple[] ar1:finalTuples)
//			{
//				System.out.println("jgchvzscvygdghcz gf dchgcz");
//				for(int l1=0;l1<numTables;l1++)
//				{
//					//ar[l1].print(in[l1]);
//				}
//			}
//			
//			for(Tuple[] ar1:finalTuples)
//			{
//				Tuple whole=new Tuple();
//				whole.setHdr((short)sumlen, sumAttr, heapstr);
//				int atcount=0;
//				int count1=0;
//				float sum = 0;
//				for(Tuple t:ar1)
//				{
//					sum+=t.getFloFld(len_in[count1]);
//					int counttu = 1;
//					 int index=0;
//					 
//				//	System.out.println("count is:"+count1);
//					Tuple t1=new Tuple();
//					t1.setHdr((short)len_in[count1], in[count1],s_sizes[count1]);
//					t1.tupleCopy(t);
//				//	t1.print(in[count1]);
//					
//					//count--;
//					//System.out.println(atcount);
//					//System.out.println("new count"+count1);
//				for(int s=0;s<len_in[count1];s++)	
//				{
//					if(sumAttr[atcount].attrType==AttrType.attrInteger)
//					{
//						
//						int c =t1.getIntFld(s+1);
//					//	System.out.println("printing int field:"+c);
//						whole.setIntFld(atcount+1, c);
//					}
//					if(sumAttr[atcount].attrType==AttrType.attrReal)
//					{
//						float c =t1.getFloFld(s+1);
//						//System.out.println("printing float field:"+c);
//						whole.setFloFld(atcount+1, c);
//					}
//					if(sumAttr[atcount].attrType==AttrType.attrString)
//					{
//						String c =t1.getStrFld(s+1);
//						//System.out.println("printing String field:"+c);
//						whole.setStrFld(atcount+1, c);
//					}
//					atcount++;
//					}count1++;
//				
//				}//after
//				whole.setFloFld(atcount+1, sum);
//				//updated content done
//				whole.print(sumAttr);
//				finalscoresk[0]=sum;
//				
//				boolean flag=true;
//				if(flag)
//				{
//					
//				ArrayList<Float> arra=new ArrayList<Float>();
//				for(float kf:finalscoresk)
//				{
//				arra.add(kf);
//				}
//				Collections.sort(arra);
//			//	Collections.so
//				for(int ke=0;ke<arra.size();ke++)
//				{
//					finalscoresk[ke]=arra.get(ke);
//				}
//				flag=false;}
				///////
//				int tempIndex=0;
//				for(int k1=0;k1<num;k1++)
//				{
//					if(tempIndex==0)
//					{
//					if(sum> finalscoresk[k1])
//					{
//						tempIndex=k1;
//					}
//					}
//				}
//				float lastseen=finalscoresk[tempIndex];
//				
//				for(int k1=tempIndex;k1<num;k1++)
//				{float temp=
//					finalscoresk[k1];
//				finalscoresk[k1]=lastseen;
//				lastseen=finalscoresk[k1];
//			
				
				
			
			
			
			
			
			//merging tuple
                
				
				
				
				
				
				
//				//merging
//				
//				Tuple tu = new Tuple();
//				tu.setHdr((short) (numTables + 2), strTypes, str);
//				// System.out.println("data printing");
//				System.out.println("string is" + key);
//				tu.setStrFld(1, key);
//				for (float[] f : al) {
//					System.out.println("new array");
//					float sum = 0;
//					int counttu = 1;
//					 int index=0;
//					 Tuple[] Tuplearr=new Tuple[numTables];
//					for (float fla : f) {
//						if(counttu!=numTables+1)
//						{
//							
//					for(int j=0;j<Tuples[counttu-1].n.size();j++)
//					{
//						Tuple e=Tuples[counttu-1].n.get(j);
//						//System.out.println("reading data from the index"+(counttu-1)+"with score"+fla );
//						if(fla==e.getScore())
//						{
//							//System.out.println("printing data with that score"+fla);
//							Tuplearr[index]=e;
//						System.out.println("index of data is:"+index);
//						Tuplearr[index].print(in[counttu-1]);
//							index++;
//						}
//					}}
////						count=0;
////						for(Tuple t:Tuplearr)
////						{
////							t.print(in[count]);
////							count++;
////						}
//					counttu++;
//						tu.setFloFld(counttu, fla);
//						sum += fla;
//						//System.out.print(fla);
//					}
//					count=0;
//					System.out.println("new tuple");
//					for(Tuple t:Tuplearr)
//					{
//						t.print(in[count]);
//						count++;
//					}
//					//whole data in this area
//					Tuple whole=new Tuple();
//					whole.setHdr((short)sumlen, sumAttr, heapstr);
//					int atcount=0;
//					int count1=0;
//					
//					for(Tuple t:Tuplearr)
//					{
//						System.out.println("count is:"+count1);
//						Tuple t1=new Tuple();
//						t1.setHdr((short)len_in[count1], in[count1],s_sizes[count1]);
//						t1.tupleCopy(t);
//						t1.print(in[count1]);
//						
//						//count--;
//						System.out.println(atcount);
//						System.out.println("new count"+count1);
//					for(int s=0;s<len_in[count1];s++)	
//					{
//						if(sumAttr[atcount].attrType==AttrType.attrInteger)
//						{
//							
//							int c =t1.getIntFld(s+1);
//							System.out.println("printing int field:"+c);
//							whole.setIntFld(atcount+1, c);
//						}
//						if(sumAttr[atcount].attrType==AttrType.attrReal)
//						{
//							float c =t1.getFloFld(s+1);
//							System.out.println("printing float field:"+c);
//							whole.setFloFld(atcount+1, c);
//						}
//						if(sumAttr[atcount].attrType==AttrType.attrString)
//						{
//							String c =t1.getStrFld(s+1);
//							System.out.println("printing String field:"+c);
//							whole.setStrFld(atcount+1, c);
//						}
//						atcount++;
//						}count1++;
//					}
//					
//					whole.setFloFld(atcount+1, sum);
//					//updated content done
//					whole.print(sumAttr);
//					
//					for(k1=0;k1<num;k1++)
//					{
//						if(tempIndex==0)
//						{
//						if(sum>k1)
//						{
//							tempIndex=k1;
//						}
//						}
//					}
//					tu.setFloFld(counttu, sum);
//					tu.print(strTypes);
//					f1.insertRecord(whole.getTupleByteArray());
//					//f1.insertRecord(tu.getTupleByteArray());
//					scorear.add(sum);
//					int tempIndex=0;
//					int k1;
//					for(k1=0;k1<num;k1++)
//					{
//						if(tempIndex==0)
//						{
//						if(sum>k1)
//						{
//							tempIndex=k1;
//						}
//						}
//					}
//					float tempscore=finalscoresk[k1-1];
//					
//					for( k1=tempIndex;k1<num;k1++)
//					{
//						float  tempscore1=0.0f;
//					    tempscore1=finalscoresk[k1];
//						finalscoresk[k1]=tempscore;
//						tempscore=tempscore1 ;
//					}
//					System.out.println("priinting the whole data of the array");
//					for(int l1=0;l1<finalscoresk.length;l1++)
//					{
//						System.out.println(finalscoresk[l1]);
//					}
//					}
//					
//				}
//				
			}

		}
		}
	
//	public Tuple get_next(){
//		
//	}

//	public Tuple get_next() throws SortException, UnknowAttrType,
//			LowMemException, JoinsException, IOException, Exception {
//		String key = new String();
//		Tuple tempTuple = new Tuple();
//		Tuple TupleTuple1 = new Tuple();
//
//		if (((TupleTuple1 = sortiter1.get_next()) != null)) {
//
//			key = (String) (TupleTuple1.getStrFld(1));
//			Tuple[] tupleArray = new Tuple[this.numTables];
//			for (int i = 0; i < numTables; i++) {
//				float rkey;
//				// float[] s_value;
//				IndexScan iscan = null;
//				int col = join_col_in[0];
//				int ikey;
//
//				System.out.println("key received by Thresold_key_By_access is:"
//						+ key);
//
//				ArrayList<float[]> al = new ArrayList<float[]>();
//
//				FldSpec[] projection = new FldSpec[in[i].length];
//				for (int j = 0; j < projection.length; j++) {
//					projection[j] = new FldSpec(new RelSpec(RelSpec.outer),
//							j + 1);
//				}
//				CondExpr[] expr = new CondExpr[2];
//				expr[0] = new CondExpr();
//				expr[0].op = new AttrOperator(AttrOperator.aopEQ);
//				expr[0].type1 = new AttrType(AttrType.attrSymbol);
//				expr[0].type2 = new AttrType(in[0][col - 1].attrType);
//				expr[0].next = null;
//				expr[1] = null;
//				if (in[0][col - 1].attrType == AttrType.attrInteger) {
//					ikey = Integer.parseInt(key);
//					expr[0].operand2.integer = ikey;
//				}
//
//				else if (in[0][col - 1].attrType == AttrType.attrReal) {
//					rkey = Float.parseFloat(key);
//					expr[0].operand2.real = rkey;
//
//				} else {
//					expr[0].operand2.string = key;
//				}
//
//				expr[0].operand1.symbol = new FldSpec(
//						new RelSpec(RelSpec.outer), join_col_in[i]);
//				int count = 0;
//				try {
//
//					iscan = new IndexScan(index[i], relNames[i], indNames[i],
//							in[i], s_sizes[i], len_in[i], len_in[i], projection, expr,
//							join_col_in[i], false);
//				} catch (Exception e) {
//
//					e.printStackTrace();
//				}
//               System.out.println("printing the data:"+i);
//               System.out.println(iscan);
//               System.out.println("key recieved data is "+iscan.get_next());
//				Tuple tu = new Tuple();
//				while ((tu = iscan.get_next()) != null) {
//					System.out.println("output of data");
//					tu.print(in[i]);
//				}
//
//			}
//
//			try {
//				resAttrTypes = new AttrType[n_out_flds];
//				for (int i = 0; i < n_out_flds; i++) {
//					resAttrTypes[i] = in[proj_list[i].relation.key][proj_list[i].offset - 1];
//
//				}
//
//				resstr = TupleUtils.setup_op_tuple(tempTuple, resAttrTypes, in,
//						len_in, s_sizes, proj_list, n_out_flds);
//
//				Projection1.Join(tupleArray, in, tempTuple, this.proj_list,
//						this.n_out_flds);// this.sortedScoreMap.get(key)
//				if (tempTuple == null) {
//					System.out.println("no data in the tuple");
//				} else {
//
//					tempTuple.print(resAttrTypes);
//				}
//				System.out.println(tempTuple.getScore());
//
//			} catch (Exception e) {
//				// TODO: handle exception
//				e.printStackTrace();
//			}
//
//		} else {
//			tempTuple = null;
//			System.out.println("print the waste data");
//		}
//		return tempTuple;
//	}

//	private Tuple getTuple(int tableIndex, String key, int attrType) {
//		Tuple tuple = null;
//		IndexScan iscan = null;
//		FldSpec[] projection = new FldSpec[len_in[tableIndex]];
//		CondExpr[] expr = new CondExpr[2];
//		expr[0] = new CondExpr();
//		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
//		expr[0].type1 = new AttrType(AttrType.attrSymbol);
//		expr[0].type2 = new AttrType(attrType);
//		expr[0].next = null;
//		expr[1] = null;
//
//		int ikey;
//		float rkey;
//
//		if (attrType == AttrType.attrInteger) {
//			ikey = Integer.parseInt(key);
//			expr[0].operand2.integer = ikey;
//		}
//
//		else if (attrType == AttrType.attrReal) {
//			rkey = Float.parseFloat(key);
//			expr[0].operand2.real = rkey;
//
//		} else {
//			expr[0].operand2.string = key;
//		}
//
//		for (int i = 0; i < len_in[tableIndex]; i++) {
//			projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
//		}
//
//		// fix the field on which to be indexed
//		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
//				join_col_in[tableIndex]);
//		Tuple tuple1 = new Tuple();
//		try {
//			iscan = new IndexScan(index[tableIndex], relNames[tableIndex],
//					indNames[tableIndex], in[tableIndex], s_sizes[tableIndex],
//					len_in[tableIndex], len_in[tableIndex], projection, expr,
//					join_col_in[tableIndex], false);
//			tuple1 = iscan.get_next();
//		} catch (Exception e) {
//
//			e.printStackTrace();
//		}
//		try {
//			// tuple = iscan.get_next();
//
//		} catch (Exception e) {
//
//			e.printStackTrace();
//		}
//
//		return tuple1;
//	}
	public Tuple get_next() throws SortException, UnknowAttrType, LowMemException, JoinsException, IOException, Exception {
		String key=new String();
		Tuple tempTuple=new Tuple();
		Tuple Tupletuple1=new Tuple();
		Tuple[] tup=new Tuple[numTables];
		
		if(((Tupletuple1=sortiter1.get_next())!=null)&&(brk!=0))
		{
			//Tupletuple1.print(sumAttr);
			//System.out.println("b4 null pointer");
			for(int i=0;i<numTables;i++)
			{
				tup[i]=new Tuple();
				tup[i].setHdr((short)len_in[i], in[i], s_sizes[i]);
			}
			
			brk--;
			int atcount=0;
			int count=0;
			for(int i=0;i<numTables;i++)
			{
				
				//tup[i].setHdr((short)len_in[i], in[i], s_sizes[i]);
				for(int j=0;j<len_in[i];j++)
				{

					if(sumAttr[atcount].attrType==AttrType.attrInteger)
					{
						int c=Tupletuple1.getIntFld(atcount+1);
				//		System.out.println("printing int field:"+c);
						tup[i].setIntFld(j+1, c);					
					}
					if(sumAttr[atcount].attrType==AttrType.attrReal)
					{
						float c =Tupletuple1.getFloFld(atcount+1);
					//	System.out.println("printing float field:"+c);
						tup[i].setFloFld(j+1, c);
					}
					if(sumAttr[atcount].attrType==AttrType.attrString)
					{
						String c =Tupletuple1.getStrFld(atcount+1);
						//System.out.println("printing String field:"+c);
						tup[i].setStrFld(j+1, c);
					}
			atcount++;
				}
				
			}
			float score=Tupletuple1.getFloFld(sumlen);
			score=score/numTables;
			for(int i=0;i<numTables;i++)
			{
			//	System.out.println("prinitng output datas");
				//tup[i].print(in[i]);
			}
			//projection;
			try {
				resAttrTypes = new AttrType[n_out_flds];
				for (int i = 0; i < n_out_flds; i++) {
					resAttrTypes[i] = in[proj_list[i].relation.key][proj_list[i].offset - 1];

				}

				resstr = TupleUtils.setup_op_tuple(tempTuple, resAttrTypes, in,
						len_in, s_sizes, proj_list, n_out_flds);

				Projection1.Join(tup, in, tempTuple, this.proj_list,
						this.n_out_flds);// this.sortedScoreMap.get(key)
				if (tempTuple == null) {
					System.out.println("no data in the tuple");
				} else {
                  //  tempTuple.setScore(score);
					tempTuple.print(resAttrTypes);
				}
				//System.out.println(tempTuple.getScore());

			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}

		} else {
			tempTuple = null;
			System.out.println("print the waste data");
		}
		return tempTuple;
			
			
		
		
		
		//return tempTuple;
	}
//	public Tuple get_next() throws SortException, UnknowAttrType, LowMemException, JoinsException, IOException, Exception {
//		String key=new String();
//		Tuple tempTuple=new Tuple();
//		Tuple TupleTuple1=new Tuple();
//		
//		if(((TupleTuple1=sortiter1.get_next())!=null) &&(brk!=0))
//		{
//		brk--;
//			key=(String)(TupleTuple1.getStrFld(1));
//		Tuple[] tupleArray = new Tuple[this.numTables];
//		 System.out.println("key recieved is "+key);
//			for (int i = 0; i < tupleArray.length; i++) {
//				  
//				}
//           
//		try {
//			resAttrTypes=new AttrType[n_out_flds];
//			for(int i=0;i<n_out_flds;i++)
//			{
//				resAttrTypes[i]=in[proj_list[i].relation.key][proj_list[i].offset-1];
//				
//			}
//
//			resstr=TupleUtils.setup_op_tuple(tempTuple,resAttrTypes, in, len_in, s_sizes, proj_list,n_out_flds);
//				
//				Projection1.Join(tupleArray,in,tempTuple, this.proj_list, this.n_out_flds);//this.sortedScoreMap.get(key)
//				if(tempTuple==null)
//				{
//					System.out.println("no data in the tuple");
//				}
//				else
//				{
//				
//				tempTuple.print(resAttrTypes);}
//			System.out.println(tempTuple.getScore());
//			
//			} catch (Exception e) {
//				// TODO: handle exception
//				e.printStackTrace();
//			}
//		
//		}
//		else
//		{
//			tempTuple=null;
//			System.out.println("print the waste data");
//		}
//		return tempTuple;
//	}
//	private Tuple getTuple(int tableIndex, String key, int attrType) {
//		Tuple tuple = null;
//		IndexScan iscan = null;
//		FldSpec[] projection = new FldSpec[len_in[tableIndex]];
//		CondExpr[] expr = new CondExpr[2];
//		expr[0] = new CondExpr();
//		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
//		expr[0].type1 = new AttrType(AttrType.attrSymbol);
//		expr[0].type2 = new AttrType(attrType);
//		expr[0].next = null;
//		expr[1] = null;
////        boolean flag1=true;
//		int ikey;
//		float rkey;
//
//		if (attrType == AttrType.attrInteger) {
//			ikey = Integer.parseInt(key);
//			expr[0].operand2.integer = ikey;
//		}
//
//		else if (attrType == AttrType.attrReal) {
//			rkey = Float.parseFloat(key);
//			expr[0].operand2.real = rkey;
//
//		} else {
//			expr[0].operand2.string = key;
//		}
//
//		for (int i = 0; i < len_in[tableIndex]; i++) {
//			projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);			
//		}
//
//		// fix the field on which to be indexed
//		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
//				join_col_in[tableIndex]);
//
//		try {
//			iscan = new IndexScan(index[tableIndex], relNames[tableIndex], indNames[tableIndex], in[tableIndex],
//					s_sizes[tableIndex], len_in[tableIndex], len_in[tableIndex], projection, expr, join_col_in[tableIndex],
//					false);
//		} catch (Exception e) {
//
//			e.printStackTrace();
//		}
//		try {
////			while(flag1)
////			{
//			tuple = iscan.get_next();
//			System.out.println("my score is:"+tuple.getScore());
//			//tuple.setHdr(0, types, strSizes);
////			if((tuple.getScore())==score)
////			{
////				flag1=false;
////			}
////			}
//		} catch (Exception e) {
//
//			e.printStackTrace();
//		}
//
//		return tuple;
//	}
//	private Tuple getTupleByScore(int tableIndex, float score, String key,
//			int attrType) {
//		Tuple tuple = null;
//		IndexScan iscan = null;
//		FldSpec[] projection = new FldSpec[len_in[tableIndex]];
//		CondExpr[] expr = new CondExpr[2];
//		expr[0] = new CondExpr();
//		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
//		expr[0].type1 = new AttrType(AttrType.attrSymbol);
//		expr[0].type2 = new AttrType(attrType);
//		expr[0].next = null;
//		expr[1] = null;
//		// boolean flag1=true;
//		int ikey;
//		float rkey;
//
//		if (attrType == AttrType.attrInteger) {
//			ikey = Integer.parseInt(key);
//			expr[0].operand2.integer = ikey;
//		}
//
//		else if (attrType == AttrType.attrReal) {
//			rkey = Float.parseFloat(key);
//			expr[0].operand2.real = rkey;
//
//		} else {
//			expr[0].operand2.string = key;
//		}
//
//		for (int i = 0; i < len_in[tableIndex]; i++) {
//			projection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
//		}
//
//		// fix the field on which to be indexed
//		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
//				join_col_in[tableIndex]);
//
//		try {
//			iscan = new IndexScan(index[tableIndex], relNames[tableIndex],
//					indNames[tableIndex], in[tableIndex], s_sizes[tableIndex],
//					len_in[tableIndex], len_in[tableIndex], projection, expr,
//					join_col_in[tableIndex], false);
//		} catch (Exception e) {
//
//			e.printStackTrace();
//		}
//		try {
//			tuple = iscan.get_next();
//			while (tuple.getScore() != score) {
//				System.out.println("score is" + tuple.getScore());
//				tuple = iscan.get_next();
//			}
//			System.out.println("my score is:" + tuple.getScore());
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return tuple;
//	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see iterator.Iterator#close()
	 */
	
	@Override
	public void close() throws IOException, JoinsException, SortException,
			IndexException {
		// TODO Auto-generated method stub

	}

}
