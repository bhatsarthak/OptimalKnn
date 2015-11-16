package main.java;

public class TestClass {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String testString = "42889*;*(40.760265, -73.989105, 'Italian', '217', '291', 'Ristorante Da Rosina')";
		String[] values=testString.split("*");
		for ( String value : values) {
			System.out.println(value);
		}
	}

}
