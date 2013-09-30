/**
 * 
 */
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
import heap.Scan;
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
import java.util.TreeMap;

import btree.BTFileScan;
import btree.BTreeFile;
import btree.DataClass;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.StringKey;
import bufmgr.PageNotReadException;

/**
 * @author beck
 *
 */
public class StreamCombine extends Iterator {

	/* (non-Javadoc)
	 * @see iterator.Iterator#get_next()
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
	private Heapfile f1;
	Sort sortiter1;
	private AttrType[] resAttrTypes,s;
	private short[] str,resstr;
	private Map<String,ArrayList<float[]>> scored;
	private int counter = 0;
	FldSpec[] proj1;
	private AttrType Key_Type; 
	AttrType[] strTypes;
	FldSpec[][] project2;
	AttrType[] sumAttr;
	short[] heapstr;
	int sumlen;
	float thresold_current=numTables;
	float[] current;
	float[] current_old;
	int tuplecount=0;
	
//	Heapfile f2=new Heapfile("scored_bck.in");
	TreeMap<Float,Integer> tm=new TreeMap<Float,Integer>();
	BTreeFile btscores=null;
	int treecount=0;
	FldSpec[] projection=null;
	IndexType b_index =null;
	CondExpr[] expr=null;
	HashSet<String> checkcontain;
	FileScan F23=null;
	/**
	 * @param numTables
	 * @param in
	 * @param len_in
	 * @param s_sizes
	 * @param join_col_in
	 * @param am
	 * @param index
	 * @param indNames
	 * @param relNames
	 * @param amt_of_mem
	 * @param outFilter
	 * @param proj_list
	 * @param n_out_flds
	 * @param num
	 * @throws UnknowAttrType
	 * @throws LowMemException
	 * @throws JoinsException
	 * @throws Exception
	 */
	public StreamCombine(int numTables, AttrType[][] in, int[] len_in,
			short[][] s_sizes, int[] join_col_in, Iterator[] am,
			IndexType[] index, String[] indNames, String[] relNames,
			int amt_of_mem, CondExpr[] outFilter, FldSpec[] proj_list,
			int n_out_flds, int num) throws UnknowAttrType, LowMemException, JoinsException, Exception {
		this.numTables = numTables;

		checkcontain=new HashSet<String>();
		project2=new FldSpec[numTables][];
		RID rid=new RID();

		for(int i=0;i<numTables;i++)
		{
			project2[i]=new FldSpec[len_in[i]];
			for(int j=0;j<len_in[i];j++)
			{
				project2[i][j]=new FldSpec(new RelSpec(RelSpec.outer),j+1);
			}
		}
		proj1=new FldSpec[numTables+2];
		for(int i=0;i<numTables+2;i++ ){
			proj1[i]=new FldSpec(new RelSpec(RelSpec.outer),i+1);
		}
		System.out.println("---------------------Stream Combineee-----------------");
		f1 = new Heapfile("scored.in");
		sumlen=0;
		// AttrType[] sumAttr=new AttrType[];
		for(int k:len_in)
		{
			sumlen+=k;
		}
		sumlen+=2;
		sumlen+=numTables;
		sumAttr=new AttrType[sumlen];
		int atcount=1;
		sumAttr[0]=new AttrType(AttrType.attrString);
		for(AttrType[] k:in)
		{
			for(AttrType p:k)
			{

				sumAttr[atcount]=p;
				atcount++;
			}
		}
		sumAttr[atcount]=new AttrType(AttrType.attrReal);
		for(int k=0;k<numTables;k++)
		{
			atcount++;
			sumAttr[atcount]=new AttrType(AttrType.attrReal);
		}
		atcount=1;
		short strlen=0;
		for(short[] l:s_sizes)
		{
			strlen+=l.length;
		}
		heapstr=new short[strlen+1];
		heapstr[0]=50;
		System.out.println("length of strings in arris");
		for(short[] l:s_sizes)
		{
			for(short p:l)
			{

				heapstr[atcount]=p;
				atcount++;
				System.out.println(p);
			}
		}

		Key_Type=in[0][join_col_in[0]-1];
		current =new float[numTables];
		for (int l = 0; l < numTables; l++) 
		{
			current[l] = 1;
		}
		current_old =new float[numTables];
		for (int n = 0; n < numTables; n++) 
		{
			current_old[n] = 1.1f;
		}
		


		this.in = new AttrType[numTables][];

		for (int i = 0; i < numTables; i++) {

			AttrType[] _in = in[i];

			this.in[i] = new AttrType[_in.length];

			System.arraycopy(_in, 0, this.in[i], 0, _in.length);

		}

		f1=new Heapfile("scored.in");
		String IndexNames="Scored.bt";
		String Relation="scored.in";
		strTypes=new AttrType[numTables+2];
		strTypes[0]=new AttrType(AttrType.attrString);

		try{
			btscores = new  BTreeFile("scored.bt", AttrType.attrString, 50, 0);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}


		//        
		//		for(int i=1;i<numTables+2;i++)
		//		{
		//			strTypes[i]=new AttrType(AttrType.attrReal);
		//		}

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
		// scored=new HashMap<String,ArrayList< float[]>>();
		Tuple t=new Tuple();
		int count=0;
		boolean flagcheck=true;
		projection=null;
		for(float i=0;i<num;i++)
		{
			tm.put((float) 0,0);
		}
		thresold_current=0;
		boolean flag=false;
		int pos=1;
		
		while(flag==false)
		{
			tuplecount++;
			Tuple temp;
			int col;
			String key = null;
			
			int i=this.StreamSelect();
				try {
					temp = am[i].get_next();
					col = join_col_in[i];
	
					temp.print(in[i]);
					current_old[i]=current[i];
					current[i]=temp.getScore();
					// TreeMap<String ,Float[]> nra=new TreeMap<String,Float[]>();

					if (in[i][col - 1].attrType == AttrType.attrInteger) {
						key = String.valueOf(temp.getIntFld(col));
					} else if (in[i][col - 1].attrType == AttrType.attrString) {
						key = temp.getStrFld(col);
					} else if (in[i][col - 1].attrType == AttrType.attrReal) {
						key = String.valueOf(temp.getFloFld(col));
					}
					if(checkcontain.contains(key))
					{
						System.out.println("key received by NRA_key_By_access is:" + key);

						projection = new FldSpec[sumlen];
						expr = new CondExpr[2];
						expr[0] = new CondExpr();
						expr[0].op = new AttrOperator(AttrOperator.aopEQ);
						expr[0].type1 = new AttrType(AttrType.attrSymbol);
						expr[0].type2 = new AttrType(sumAttr[0].attrType);
						expr[0].next = null;
						expr[1]  = null;// while((tup=sortiter1.get_next())!=null)// while((tup=sortiter1.get_next())!=null)
						expr[0].operand2.string = key;
						expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
								1);
						projection=new FldSpec[sumlen];
						for(int j=0;j<sumlen;j++)
						{

							projection[j] = new FldSpec(new RelSpec(RelSpec.outer),
									j+1);
						}
						b_index = new IndexType(IndexType.B_Index);
						IndexScan iscan = null;
						try
						{
							iscan = new IndexScan(b_index, "scored.in","scored.bt" ,
									sumAttr, heapstr, sumlen,sumlen,projection, expr,
									1, false);//
						}
						catch(Exception e)
						{
							e.printStackTrace();

						}
						
					
//						Tuple tf1=new Tuple();
//						tf1.setHdr((short)sumlen, sumAttr, heapstr);
//						FileScan test=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);
//						System.out.println("after iscannn");
//						while((tf1=test.get_next())!=null)
//						{
//							tf1.print(sumAttr);
//						}

						Tuple tf=new Tuple();
						tf.setHdr((short)sumlen, sumAttr, heapstr);

						Tuple t2=new Tuple();
						t2.setHdr((short)sumlen, sumAttr, heapstr);
						if((t2=iscan.get_next())!=null)
						{
							System.out.println();
							tf.tupleCopy(t2);
							int indexofsc=0;
							int indexcheck=0;
							for(int j=0;j<i;j++)
							{
								indexofsc+=len_in[j];
							}
							t2.print(sumAttr);
							indexofsc++;
							indexcheck=sumlen-(numTables-(i+1));
							float score=tf.getFloFld(indexcheck);//may not work check for the other values
							if(score==-1)
							{
								//								indexofsc-=len_in[i];
								for(int j=indexofsc+1;j<indexofsc+len_in[i]+1;j++)
								{
									if(sumAttr[j-1].attrType==AttrType.attrInteger)
									{
										tf.setIntFld(j, temp.getIntFld(j-indexofsc));
									}
									if(sumAttr[j-1].attrType==AttrType.attrString)
									{
										tf.setStrFld(j, temp.getStrFld(j-indexofsc));
									}
									if(sumAttr[j-1].attrType==AttrType.attrReal)
									{
										tf.setFloFld(j, temp.getFloFld(j-indexofsc));
									}
								}
								tf.setFloFld(indexcheck,temp.getScore());
								float tupscore=0.0f;
								tupscore=tf.getFloFld(sumlen-numTables);

								RID r;

								r=new RID(iscan.getrid().pageNo,iscan.getrid().slotNo);



								System.out.println("score:"+tf.getFloFld(sumlen-numTables));
								float combined_score=0;
								for(int c=1;c<numTables+1;c++)
								{
									if((tf.getFloFld((sumlen-numTables)+c)!=-1))
										combined_score+=tf.getFloFld((sumlen-numTables)+c);

								}


								//tf.setFloFld((sumlen-numTables), temp.getScore()+tf.getFloFld(sumlen-numTables));

								tf.setFloFld((sumlen-numTables),combined_score);
								System.out.println(f1.deleteRecord(r));
								btscores.Delete(new StringKey(tf.getStrFld(1)), r);
								tf.print(sumAttr);
								RID r1=new RID();

								r1=f1.insertRecord(tf.getTupleByteArray());
								//btscores.Delete(new StringKey(tf.getStrFld(1)), r1);
								btscores.insert(new StringKey(tf.getStrFld(1)), r1)	;		
								Tuple tocheck=new Tuple();
								tocheck.setHdr((short)sumlen,sumAttr,heapstr);
								tocheck.tupleCopy(tf);
								if(isFilled(tf))
								{
									//flag=  breaking_condition(tf.getFloFld(sumlen-numTables));
//									FileScan F2=null;
//									Sort sortiter1 = null;
//									FileScan F_23=null;
									
										FileScan F_23=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);


										Tuple p=new Tuple();
										Tuple p1=new Tuple();
								        p1.setHdr((short)sumlen,sumAttr,heapstr);
										p.setHdr((short)sumlen,sumAttr,heapstr);
										System.out.println("after updatingggg");
										while((p=F_23.get_next())!=null)
										{
											p1.tupleCopy(p);
											float max=0.0f;
											for(int t1=1;t1<numTables+1;t1++)
											{
												float tempscore=p1.getFloFld((sumlen-numTables)+t1);
												if(tempscore==-1)
												{
													tempscore=current[t1-1];
												}
												max+=tempscore;
											}
											p1.setFloFld(sumlen-numTables, max)	;	

											p1.print(sumAttr);
											try{
											RID r_b;
											r_b=new RID(F_23.getrid().pageNo,F_23.getrid().slotNo);
										
									     System.out.println(f1.deleteRecord(r_b));
									     btscores.Delete(new StringKey(p1.getStrFld(1)), r_b);
											RID r1_b;
											
											r1_b=f1.insertRecord(p1.getTupleByteArray());	
										btscores.insert(new StringKey(p1.getStrFld(1)), r1_b);
											}
											catch(Exception e)
											{
												e.printStackTrace();
											}
											
										}
										
										

									

									FileScan F2=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);

									Sort sortiter1=new Sort(sumAttr,(short)sumlen,heapstr,F2,sumlen-numTables,new TupleOrder(TupleOrder.Descending),4,10);

										Tuple t12=new Tuple();
										Tuple tp=new Tuple();
										t12.setHdr((short)sumlen,sumAttr, heapstr);
										tp.setHdr((short)sumlen, sumAttr, heapstr);
										System.out.println(" after sortingggggggggggggggggggg");

										int n=num;
										while((t12=sortiter1.get_next())!=null)
										{
											t12.print(sumAttr);
											if(n<1)
											{
												break;
											}

											n--;
										}
										//F_23.close();		
										

										if(t12==null)
										{
											flag=false;
										}
										else if(t12.getFloFld(sumlen-numTables)>tocheck.getFloFld(sumlen-numTables))
										{
											flag= true;
										}
										else
										{
											flag= false;
										}

										sortiter1.close();

										F2.close();

								}

							}

							else
							{
								//duplicate record condition****************

								Tuple t_dup=new Tuple();
								t_dup.setHdr((short)sumlen, sumAttr, heapstr);
								t_dup.tupleCopy(tf);
								float lastscore=0.0f;
								int indexst=0;
								for(int j=0;j<i;j++)
								{
									indexst+=len_in[j];
								}
								indexst++;

								for(int j=indexst+1, k=1;k<len_in[i]+1;j++,k++)//j<indexst+len_in[i]
								{
									if(sumAttr[j-1].attrType==AttrType.attrInteger)
									{
										t_dup.setIntFld(j, temp.getIntFld(k));//	tf.setIntFld(j, temp.getIntFld(j-indexst));
									}
									else if(sumAttr[j-1].attrType==AttrType.attrString)
									{
										t_dup.setStrFld(j, temp.getStrFld(k));//	tf.setStrFld(j, temp.getStrFld(j-indexst));
									}
									else if(sumAttr[j-1].attrType==AttrType.attrReal)
									{
										t_dup.setFloFld(j, temp.getFloFld(k));//tf.setFloFld(j, temp.getFloFld(j-indexst));
									}
								}
								lastscore=t_dup.getFloFld(indexcheck);
								t_dup.setFloFld(indexcheck,temp.getScore());
								/*
								 * 
								r=new RID(iscan.getrid().pageNo,iscan.getrid().slotNo);



                                System.out.println("score:"+tf.getFloFld(sumlen-numTables));
                                float combined_score=0;
                                for(int c=1;c<numTables+1;c++)
                                {
                                	if((tf.getFloFld((sumlen-numTables)+c)!=-1))
                                		combined_score+=tf.getFloFld((sumlen-numTables)+c);

                                }


								//tf.setFloFld((sumlen-numTables), temp.getScore()+tf.getFloFld(sumlen-numTables));

                                tf.setFloFld((sumlen-numTables),combined_score);
								System.out.println(f1.deleteRecord(r));
								tf.print(sumAttr);

								RID r1=f1.insertRecord(tf.getTupleByteArray());
								//btscores.Delete(new StringKey(tf.getStrFld(1)), r1);
                              btscores.insert(new StringKey(tf.getStrFld(1)), r1)	;
								 */
								t_dup.setFloFld(sumlen-numTables, temp.getScore()+tf.getFloFld(sumlen-numTables)-lastscore);
								RID rid_1=new RID();
								rid_1=f1.insertRecord(t_dup.getTupleByteArray());
								btscores.insert(new StringKey(t_dup.getStrFld(1)), rid_1);
							}


						} 
						iscan.close();
					}
					else
					{
						checkcontain.add(key);
						Tuple tf1=new Tuple();
						tf1.setHdr((short)sumlen, sumAttr, heapstr);
						tf1.setStrFld(1, key);
						for(int  j=1;j<sumlen;j++)
						{
							if(sumAttr[j].attrType==AttrType.attrInteger)
							{
								tf1.setIntFld(j+1, -1);
							}
							if(sumAttr[j].attrType==AttrType.attrString)
							{
								tf1.setStrFld(j+1, "val");
							}
							if(sumAttr[j].attrType==AttrType.attrReal)
							{
								tf1.setFloFld(j+1, -1.0f);
							}

						}
						tf1.print(sumAttr);
						int indexst=0;
						for(int j=0;j<i;j++)
						{
							indexst+=len_in[j];
						}
						indexst++;

						for(int j=indexst+1, k=1;k<len_in[i]+1;j++,k++)//j<indexst+len_in[i]
						{
							if(sumAttr[j-1].attrType==AttrType.attrInteger)
							{
								tf1.setIntFld(j, temp.getIntFld(k));//	tf.setIntFld(j, temp.getIntFld(j-indexst));
							}
							else if(sumAttr[j-1].attrType==AttrType.attrString)
							{
								tf1.setStrFld(j, temp.getStrFld(k));//	tf.setStrFld(j, temp.getStrFld(j-indexst));
							}
							else if(sumAttr[j-1].attrType==AttrType.attrReal)
							{
								tf1.setFloFld(j, temp.getFloFld(k));//tf.setFloFld(j, temp.getFloFld(j-indexst));
							}

						}
						tf1.setFloFld(sumlen-(numTables-(i+1)), temp.getScore());
						tf1.setFloFld(sumlen-(numTables), temp.getScore());

						System.out.println("this is not the accurate information");
						tf1.print(sumAttr);
						try{
						RID	rid1=new RID();
						rid1=f1.insertRecord(tf1.returnTupleByteArray());
//							System.out.println(key);
//							System.out.println(tf1.getStrFld(1));
							btscores.insert(new StringKey(tf1.getStrFld(1)), rid1);
							BTFileScan n=btscores.new_scan(null, null);
                            KeyDataEntry k=null;
                            while((k=n.get_next())!=null)
                            {
                         	   Object p1=k.key;
                         	   //DataClass r=k.data;
                         	   //Object d=;
                         	   //RID r=
                         	   
                         	   
                         	 //  System.out.println("key inserted inti btree"+p1.toString()+"rid is page no"+r.pageNo+"slot.no"+r.slotNo);
                         	   
                            }
						
							System.out.println("page no:"+rid1.pageNo);
							System.out.println(rid1.slotNo);
//							Tuple te=new Tuple();
//							te.setHdr((short)sumlen, sumAttr, heapstr);
//							Scan s=f1.openScan();
//							te=s.getNext(rid1);
//							te.setHdr((short)sumlen, sumAttr, heapstr);
//							System.out.println("after adding into the heapfile for the first time");
//							te.print(sumAttr);
							
//							te=f1.getRecord(rid1);
//							te.setHdr((short)sumlen, sumAttr, heapstr);

						}
						catch(Exception e)
						{
							e.printStackTrace();
						}

					}

				}				
				catch(Exception e)
				{
					e.printStackTrace();

				}
			
		}			
		System.out.println("stopping condition reached");


		FileScan F1234=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);
		Tuple tuple1234=new Tuple();


		int cuf=0;
		FileScan F12345=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);
		Tuple tuple12345=new Tuple();
		tuple12345.setHdr((short)sumlen,sumAttr, heapstr);
		while((tuple12345=F12345.get_next())!=null)
		{
			System.out.println("tuple selected");
			tuple12345.print(sumAttr);
			boolean flag1=true;
			for(int t1=1;t1<numTables+1;t1++)
			{
				float tempscore=tuple12345.getFloFld((sumlen-numTables)+t1);
				if(tempscore==-1)
				{
					flag1=false;
				}

			}


			System.out.println("status of tuple filled:"+flag1);
			if(!flag1)
			{
				cuf++;
			}

		}
		System.out.println("number of unfilled tuples:"+cuf);
		while(cuf!=0)
		{
			Tuple temp=new Tuple();
			int col=0;
			String key=null;
			for (int i = 0; i < numTables; i++) {
				try {
					temp = am[i].get_next();
					col = join_col_in[i];
					temp.print(in[i]);	
					current[i]=temp.getScore();
					// TreeMap<String ,Float[]> nra=new TreeMap<String,Float[]>();

					if (in[i][col - 1].attrType == AttrType.attrInteger) {
						key = String.valueOf(temp.getIntFld(col));
					} else if (in[i][col - 1].attrType == AttrType.attrString) {
						key = temp.getStrFld(col);
					} else if (in[i][col - 1].attrType == AttrType.attrReal) {
						key = String.valueOf(temp.getFloFld(col));
					}
					if(checkcontain.contains(key))
					{
						System.out.println(" join key"+key);
						System.out.println(" cuf "+cuf);
						expr[0] = new CondExpr();
						expr[0].op = new AttrOperator(AttrOperator.aopEQ);
						expr[0].type1 = new AttrType(AttrType.attrSymbol);
						expr[0].type2 = new AttrType(sumAttr[0].attrType);
						expr[0].next = null;
						expr[1]  = null;// while((tup=sortiter1.get_next())!=null)// while((tup=sortiter1.get_next())!=null)
						expr[0].operand2.string = key;
						expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),
								1);
						//scan
						IndexScan iscan = null;
						try
						{
							iscan = new IndexScan(b_index, "scored.in","scored.bt" ,
									sumAttr, heapstr, sumlen,sumlen,projection, expr,
									1, false);//
						}
						catch(Exception e)
						{
							e.printStackTrace();

						}
						Tuple p=new Tuple();
						p.setHdr((short)sumlen, sumAttr, heapstr);

						Tuple tf=new Tuple();
						tf.setHdr((short)sumlen, sumAttr, heapstr);
						while((tf=iscan.get_next())!=null)
						{


							//tf.tupleCopy(p);
							p.tupleCopy(tf);
							p.print(sumAttr);
							int indexofsc=0;
							int indexcheck=0;

							for(int j=0;j<i;j++)
							{
								indexofsc+=len_in[j];
							}

							indexofsc++;
							indexcheck=sumlen-(numTables-(i+1));
							float score=p.getFloFld(indexcheck);//may not work check for the other values
							System.out.println("score value in fillingg"+score);
							if(score==-1)
							{



								//									indexofsc-=len_in[i];
								for(int j=indexofsc+1;j<indexofsc+len_in[i]+1;j++)
								{
									if(sumAttr[j-1].attrType==AttrType.attrInteger)
									{
										p.setIntFld(j, temp.getIntFld(j-indexofsc));
									}
									if(sumAttr[j-1].attrType==AttrType.attrString)
									{
										p.setStrFld(j, temp.getStrFld(j-indexofsc));
									}
									if(sumAttr[j-1].attrType==AttrType.attrReal)
									{
										p.setFloFld(j, temp.getFloFld(j-indexofsc));
									}
								}
								p.setFloFld(indexcheck,temp.getScore());
								float tupscore=0.0f;
								tupscore=p.getFloFld(sumlen-numTables);

								p.setFloFld((sumlen-numTables), temp.getScore()+p.getFloFld(sumlen-numTables));
								System.out.println("------------after filling---------------");
								p.print(sumAttr);
								RID r = new RID();
								//						System.out.println("rid before"+	r.toString());
								r=iscan.getrid();
								//						System.out.println("rid afterrr"+r.toString());
								//							
								//							boolean ex=f1.updateRecord(r, p);
								//							System.out.println("update record statussssssssss:"+ex);
								f1.deleteRecord(r);
								
								RID r2=new RID();								
								r2=f1.insertRecord(p.getTupleByteArray());
								//btscores.Delete(new StringKey(key), r);
								btscores.insert(new StringKey(key), r2);
                               BTFileScan n=btscores.new_scan(null, null);
                               KeyDataEntry k=null;
                               while((k=n.get_next())!=null)
                               {
                            	   Object p1=k.key;
                            	   //DataClass r=k.data;
                            	   System.out.println(p1.toString());
                            	   
                               }
                               
                               
							}



						}
						iscan.close();

//						Tuple min1=new Tuple();
//						min1.setHdr((short)sumlen,sumAttr, heapstr);
//						System.out.println("current values"+current[0]+" , "+current[1]);
//						FileScan F_1=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);
//						min1=minTopK(F_1);
//
//						System.out.println("the min tuple is ");
//						min1.print(sumAttr);
//
//						F1234=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);
//						prune(F1234,min1);
//						F_1.close();
//
//						F12345=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);
//						tuple1234=new Tuple();
//						tuple1234.setHdr((short)sumlen,sumAttr, heapstr);
//						System.out.println("==============after pruning=================");
//						while((tuple1234=F12345.get_next())!=null)
//						{
//							tuple1234.print(sumAttr);
//						}
//						F12345.close();
						cuf=0;
						FileScan F123456=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);
						Tuple tuple_12345=new Tuple();
						tuple_12345.setHdr((short)sumlen,sumAttr, heapstr);
						while((tuple_12345=F123456.get_next())!=null)
						{
							System.out.println("tuple selected");
							tuple_12345.print(sumAttr);
							boolean flag1=true;
							for(int t1=1;t1<numTables+1;t1++)
							{
								float tempscore=tuple_12345.getFloFld((sumlen-numTables)+t1);
								if(tempscore==-1)
								{
									flag1=false;
								}

							}


							System.out.println("status of tuple filled:"+flag1);
							if(!flag1)
							{
								cuf++;
							}

						}
						System.out.println("number of unfilled tuples:"+cuf);
						F1234.close();


					}


				}catch(Exception e)
				{
					e.printStackTrace();
				}

			}

		}
		System.out.println("All the tuples are filled");



	}

	public boolean breaking_condition(float s) throws SortException, UnknowAttrType, LowMemException, JoinsException, IOException, Exception
	{
		FileScan F2=null;
		Sort sortiter1 = null;
		FileScan F_23=null;
		try{
			F_23=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);


			Tuple p=new Tuple();
			boolean flag=true;
			int pos=0;
			p.setHdr((short)sumlen,sumAttr,heapstr);
			System.out.println("after updatingggg");
			while((p=F_23.get_next())!=null)
			{
				float max=0.0f;
				for(int t1=1;t1<numTables+1;t1++)
				{
					float tempscore=p.getFloFld((sumlen-numTables)+t1);
					if(tempscore==-1)
					{
						tempscore=current[t1-1];
					}
					max+=tempscore;
				}
				p.setFloFld(sumlen-numTables, max)	;	

				p.print(sumAttr);
				RID r;
				r=F_23.r;
				f1.deleteRecord(r);
				RID r1;
				r1=f1.insertRecord(p.getTupleByteArray());
				btscores.insert(new StringKey(p.getStrFld(1)), r1);
			}



			F2=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);

			sortiter1=new Sort(sumAttr,(short)sumlen,heapstr,F2,sumlen-numTables,new TupleOrder(TupleOrder.Descending),4,10);

			Tuple t12=new Tuple();
			Tuple tp=new Tuple();
			t12.setHdr((short)sumlen,sumAttr, heapstr);
			tp.setHdr((short)sumlen, sumAttr, heapstr);
			System.out.println(" after sortingggggggggggggggggggg");

			int n=num;
			while((t12=sortiter1.get_next())!=null)
			{
				if(n<1)
				{
					break;
				}

				n--;
			}
			//F_23.close();		
			sortiter1.close();

			F2.close();

			if(t12==null)
			{
				return false;
			}
			if(t12.getFloFld(sumlen-numTables)>s)
			{
				return true;
			}
			else
			{return false;}

		}

		finally{



		}

	}




	public boolean isFilled(Tuple tocheck) throws FieldNumberOutOfBoundException, IOException
	{
		boolean flag=true;
		for(int t1=1;t1<numTables+1;t1++)
		{
			float tempscore=tocheck.getFloFld((sumlen-numTables)+t1);
			if(tempscore==-1)
			{
				flag=false;
			}
		}
		return flag;
	}


	public Tuple minTopK(FileScan i_min) throws UnknowAttrType, LowMemException, JoinsException, Exception
	{
		System.out.println("sum lenthhh for sort inside mintopkkkk"+sumlen);
		//FileScan F_2=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);
		Sort sortiter=new Sort(sumAttr,(short)sumlen,heapstr,i_min,sumlen-numTables,new TupleOrder(TupleOrder.Descending),4,10);

		Tuple t12=new Tuple();
		Tuple tp=new Tuple();
		t12.setHdr((short)sumlen,sumAttr, heapstr);
		tp.setHdr((short)sumlen, sumAttr, heapstr);
		for(int count4=0; count4<num;count4++)
		{
			if((t12=sortiter.get_next())!=null)
			{

				tp.tupleCopy(t12); 
				t12.print(sumAttr);
			}
		}

		//F_2.close();
		sortiter.close();
		return tp;
	}

	public void sort_heapfile() throws SortException, UnknowAttrType, LowMemException, JoinsException, Exception
	{
		Tuple st=new Tuple();
		st.setHdr((short)sumlen,sumAttr, heapstr);
		Heapfile s1=new Heapfile("scored1.in");
		FileScan s=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);
		while((st=s.get_next())!=null)
		{
			s1.insertRecord(st.getTupleByteArray());
		}
		FileScan s_dup=new FileScan("scored1.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);
		sortiter1=new Sort(sumAttr,(short)sumlen,heapstr,s_dup,sumlen-numTables,new TupleOrder(TupleOrder.Descending),4,10);
		Tuple t_12=new Tuple();
		t_12.setHdr((short)sumlen,sumAttr, heapstr);

	}

	public void prune (FileScan i,Tuple t_min) throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception		

	{
		Tuple p=new Tuple();

		p.setHdr((short)sumlen,sumAttr,heapstr);
		while((p=i.get_next())!=null)
		{
			float max=0.0f;
			for(int t=1;t<numTables+1;t++)
			{
				float tempscore=p.getFloFld((sumlen-numTables)+t);
				if(tempscore==-1)
				{
					tempscore=current[t-1];
				}
				max+=tempscore;
			}
			p.print(sumAttr);
			System.out.println("Tuple  has max score"+max);
			Tuple t12=new Tuple();
			t12.setHdr((short)sumlen,sumAttr,heapstr);
			t12.tupleCopy(t_min);
			//System.out.println("min tuple recevied by prune");
			//t12.print(sumAttr);
			float min=t12.getFloFld(sumlen-numTables);
			System.out.println("the min score is "+min);
			if(min>max)	
			{
				String remove=t12.getStrFld(1);
				checkcontain.remove(remove);

				RID r=i.r;
				f1.deleteRecord(r);

			}			

		}	

	}

public int StreamSelect() throws UnknowAttrType, LowMemException, JoinsException, Exception
{
     
	int i=0;
	float[] m=new float[numTables];
	float[] d=new float[numTables];
	double[] ss=new double[numTables];
	int icount=0;
	if(tuplecount>numTables)
	{
	
	icount=0;
	for(float k:m)
	{
		//k=current_old[count]-current[count];
		k=0;
		icount++;
	}
	icount=0;

	for(double k:ss)
	{
		k=0;
	}
	projection=new FldSpec[sumlen];
   for(int k=0;k<sumlen;k++)
   {
	   projection[k]=new FldSpec(new RelSpec(RelSpec.outer),k+1);
   }
	
	
	Tuple t=new Tuple();
	Tuple t_dup=new Tuple();
	t_dup.setHdr((short) sumlen, sumAttr, heapstr);
	t.setHdr((short) sumlen, sumAttr, heapstr);
	FileScan s=new FileScan("scored.in",sumAttr,heapstr,(short)sumlen,sumlen,projection,null);
	Sort sortiter=new Sort(sumAttr,(short)sumlen,heapstr,s,sumlen-numTables,new TupleOrder(TupleOrder.Descending),4,10);
	int count=0;
	while((count<num)&&(t=sortiter.get_next())!=null)
	{
		
		 t_dup.tupleCopy(t);
	     System.out.println("tuple selected:");
	     t_dup.print(sumAttr);
		 int f=sumlen-numTables;
		 for(int j=1;j<numTables+1;j++)
		 {
			float temp_score=t_dup.getFloFld(f+j);
			if(temp_score==-1)
			{
				m[j-1]++;
			}
			System.out.println("m would be");
			for(float p:m)
			{
				
				System.out.println(p);
			}
		 }
		count++;
	}
	
	sortiter.close();
	s.close();
	int min=0;
	 count=0;
	 icount=0;
//	 for(float k:d)
//		{
//			k=current_old[icount]-current[icount];
//		System.out.println("d["+icount+"]"+k);
//			icount++;
//		}
	 for(int j=0;j<numTables;j++)
	 {
			d[j]=current_old[j]-current[j];
			System.out.println("d["+j+"]"+d[j]);
				//icount++;
	 }
	for(int j=0;j<numTables;j++)
	{
		System.out.println("dcoiuint"+d[j]+"mcoutn"+m[j]);
		ss[j]=m[j]*d[j];
		System.out.println("ss is"+count+":"+ss[j]);
		count++;
	}
	
	double minc=0;
	
	for(int j=0;j<numTables;j++)
	{
		if(minc<ss[j])
		{
			minc=ss[j];
					min=j;
					System.out.println(minc+"min updated"+min);
		}
	}
	
	
	return min;}
	return 0;
}










	public Tuple get_next() throws SortException, UnknowAttrType, LowMemException, JoinsException, IOException, Exception {
		String key=new String();
		Tuple tempTuple=new Tuple();
		Tuple Tupletuple1=new Tuple();
		Tuple[] tup=new Tuple[numTables];
		Tupletuple1.setHdr((short)sumlen, sumAttr, heapstr);
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
			int atcount=1;
			int count=0;
			for(int i=0;i<numTables;i++)
			{

				//tup[i].setHdr((short)len_in[i], in[i], s_sizes[i]);
				for(int j=0;j<len_in[i];j++,atcount++)
				{

					if(sumAttr[atcount].attrType==AttrType.attrInteger)
					{
						int c=Tupletuple1.getIntFld(atcount+1);
						//System.out.println("printing int field:"+c);
						tup[i].setIntFld(j+1, c);					
					}
					else if(sumAttr[atcount].attrType==AttrType.attrReal)
					{
						float c =Tupletuple1.getFloFld(atcount+1);
						//System.out.println("printing float field:"+c);
						tup[i].setFloFld(j+1, c);
					}
					else if(sumAttr[atcount].attrType==AttrType.attrString)
					{
						String c =Tupletuple1.getStrFld(atcount+1);
						//System.out.println("printing String field:"+c);
						tup[i].setStrFld(j+1, c);
					}
					//atcount++;
				}

			}
			float score=Tupletuple1.getFloFld(sumlen);
			score=score/numTables;
			for(int i=0;i<numTables;i++)
			{
				//System.out.println("prinitng output datas");
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
					tempTuple.setScore(score);
					//tempTuple.print(resAttrTypes);
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



	private Tuple getTupleByScore(int tableIndex,float score, String key, int attrType) {
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
		//        boolean flag1=true;
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
			tuple=iscan.get_next();
			while(tuple.getScore()!=score){
				tuple = iscan.get_next();
			}
			System.out.println("my score is:"+tuple.getScore());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tuple;
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