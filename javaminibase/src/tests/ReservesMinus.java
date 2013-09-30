package tests;

// Define the Reserves schema
class ReservesMinus {
	public int sid;
	public int bid;
	public String date;
	public float score;

	public ReservesMinus(int _sid, int _bid, String _date, float score) {
		sid = _sid;
		bid = _bid;
		date = _date;
		this.score=score;

	}
}