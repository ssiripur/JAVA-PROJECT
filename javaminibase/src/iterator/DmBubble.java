package iterator;

import java.util.ArrayList;

public class DmBubble {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		// TODO Auto-generated method stub

		ArrayList<Integer> am=new ArrayList<Integer>();
		am.add(19);
		am.add(18);
		am.add(17);
	    am.add(16);
		am.add(15);
		am.add(14);
		am.add(13);
		am.add(13);
		am.add(12);
		
	for(int i=0;i<9;i++)
	{
		for(int j=1;j<9;j++)
		{
			if(am.get(j)<am.get(j-1))
			{
				int temp=am.get(j);
				am.set(j,am.get(j-1));
				am.set(j-1, temp);
			}
		}
	}
		for(int i=0;i<am.size();i++)
		{
			System.out.println(am.get(i));
		}
		
		
		
		
		
	}

}
