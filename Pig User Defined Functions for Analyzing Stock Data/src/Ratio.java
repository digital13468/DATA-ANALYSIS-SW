import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
/**
 * A simple UDF that divides the second value by the first value. 
 * It can be used in a Pig Latin script as Ratio(x, y), where x and y are both expected to be doubles. 
 * @author Chan-Ching Hsu
 *
 */

public class Ratio extends EvalFunc<Double> {
	
	public Double exec(Tuple input) throws IOException {
		try {
			/* Rather than give explicit arguments, UDFs are handed a tuple. 
			 * The UDF must know the arguments it expects and pull them out of the tuple.
			 * Theses next lines get the first and second fields out of the input tuple that was handed in.
			 * Since Tuple.get returns Objects, we cast them to Doubles. 
			 * If the casting fails, an exception will be thrown.
			 */
			// Check that we were passed two fields
			if (input.size() != 2) {
				/*String msg = "(";
				for (int i = 0; i < input.size(); i ++) {
					msg += (String) (input.get(i)) + " ";
				}*/
				//throw new RuntimeException("Expected (double, double), input does not have 2 fields: " + msg + ")");
				throw new RuntimeException("Expected (double, double), input has " + input.size() + " fields." + input.get(0));
			}
				
			double denominator = (double) input.get(0);
			double numerator = (double) input.get(1);
			double result = numerator / denominator;
			return result;
		} catch (Exception e) {
			//Throwing an exception will cause the task to fail.
			throw new IOException(e);
		}
	}
}
