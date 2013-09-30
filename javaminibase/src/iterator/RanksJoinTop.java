package iterator;
import heap.*;
import global.*;
import btree.AddFileEntryException;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import bufmgr.*;
import diskmgr.*;
import index.*;

import java.lang.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.*;


public class RanksJoinTop {
	BTreeFile[] bt;
	Tuple[] tempTuple;  
	short[] s;
	RanksJoinTop(int numTables,
			AttrType[][] in,
			int[] len_in,
			short[][] s_sizes,
			int[] join_col_in,
			Iterator[] am,
			IndexType[] index,
			java.lang.String[] relnames,
			java.lang.String[] indNames,
			int amt_of_mem,
			CondExpr[] outFilter,
			FldSpec[] proj_list,
			int n_out_flds,
			int num
			) throws GetFileEntryException, ConstructPageException, AddFileEntryException, IOException
	{
		s=new short[1];
		for(int i=0;i<in[0].length;i++)
			{
				if(in[0][i].attrType == AttrType.attrString &&i==join_col_in[0])
				{ 
					break;
				}
				s[0]=s_sizes[0][i];
				
			}
		
		IndexType b_index=new IndexType(IndexType.B_Index);
		
		AttrType[] attr;
		int tIndex=0;
		FileScan[] ak=new FileScan[numTables];
		FileScan a;
		Heapfile f;
		Tuple temp1;
		int count_reached=0;
	    String key;
		bt=new BTreeFile[numTables];
		f=new Heapfile("temper.in");
		for(int i=0;i<numTables;i++)
		{
			tempTuple[i].setHdr((short)len_in[i], in[i], s_sizes[i]);
		}
	
	//	a=new FileScan(,);
		attr[0].attrType=AttrType.attrString;
		
		for(int i=1;i<numTables+1;i++)
		{
		attr[i].attrType=AttrType.attrReal;
		}
		IndexScan am1=new IndexScan(b_index,"temper.in","BTreeIndex",attr,s,numTables+1,numTables+1,null,null,0,false);
		//a=new FileScan("temper.in",attr,s_sizes[tIndex],(short)len_in[tIndex],1,n,null);
		FldSpec[]  n=new FldSpec[1];
		for(tIndex=0;tIndex<numTables;tIndex++)
		{ 
			n[0]=new FldSpec((new RelSpec(RelSpec.outer)),join_col_in[tIndex]);
			bt[tIndex]=new BTreeFile(relnames[tIndex],in[tIndex][join_col_in[tIndex]].attrType,100,1);
			ak[tIndex]= new FileScan(relnames[tIndex],in[tIndex],s_sizes[tIndex],(short)len_in[tIndex],1,n,null);
		}
		
		while(count_reached<num)
		{
			temp1	= am[tIndex].get_next();
			tempTuple[tIndex].tupleCopy(temp1) ;
			key=tempTuple[tIndex].getStrFld(join_col_in[tIndex]);
			//(ak[tIndex].get_next());

		}
		
	tIndex=(tIndex+1)%numTables;}
		
	}

}
