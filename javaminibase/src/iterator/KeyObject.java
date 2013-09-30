package iterator;

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.io.*;

public class KeyObject {
	public  float score1;
	public  int tableNum;
	public  Tuple tuple1;
	
	KeyObject()
	{
		
	}

	public KeyObject(float score1,int tableNum,Tuple tuple1)
	{
		this.score1=score1;
		this.tableNum=tableNum;
		this.tuple1=tuple1;
	}

}
