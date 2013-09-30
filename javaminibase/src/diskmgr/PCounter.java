package diskmgr;

public class PCounter {

  public static int counter = 0;

  public static void initialize() {
      counter =0;
	}
  public static void increment() {
      counter++;
    }
  
  public static int getPCount() {
	  return counter;
  }
  
  public static void println() {
	  System.out.println("PCount is : " + counter);
  }
}
