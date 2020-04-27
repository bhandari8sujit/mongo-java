import static org.junit.jupiter.api.Assertions.*;

import java.util.Random;

import org.junit.jupiter.api.Test;

class test {

	@Test
	void test() {
		System.out.println(String.format("%09d", new Integer(123456789)));
		System.out.println("hello,'world',!");
		Random random = new Random();
		String str2 = String.format("My answer is %.8f", 47.65734);
		System.out.println(str2);
		System.out.println(String.format("%.2f", (random.nextInt(1099999)-99999)/100.0));
	}

}
