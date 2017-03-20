import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;


public class CustomMovingAverage extends EvalFunc<DataBag> {
	public int startDate = 20131001;	// start date
	public int endDate = 20131031;	// end date
	public int windowSize = 20;	// n-day moving average
	TupleFactory mTupleFactory = TupleFactory.getInstance();
	BagFactory mBagFactory = BagFactory.getInstance();
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
				return null;
		try {
			
			DataBag dataBag = (DataBag) input.get(0);
			HashMap<Object, Object> map = new HashMap<Object, Object>();	// key = date, value = price
			for (Tuple t : dataBag)
				if (t.size() == 2)
					if (t.get(0) != null && t.get(1) != null)
						map.put(t.get(0), t.get(1));
					else
						throw new RuntimeException("input is not valid. (" + t.get(0) + ", " + t.get(1) + ")");
				else
					throw new RuntimeException("input has " + t.size() + " fields.");
			ArrayList<Integer> previousDates = new ArrayList<Integer>();	// for previous n-day dates
			ArrayList<Double> previousPrices = new ArrayList<Double>();	// for prices in previous n days
			
			int currentDate = startDate - 1;
			//int maxSearchingDays = windowSize;
			for (int i = windowSize - 1; i >= 0 ; i --) {
				if (!map.containsKey(currentDate)) {	// if data for the date not available, skip
					i ++;
					if (currentDate % 100 == 0) {	//	deal with the range across months
						String year = String.valueOf(currentDate).substring(0, 4);
						String month = String.valueOf(currentDate - 100).substring(4, 6);
						String date;
						if (month.equals("00")) {
							year = String.valueOf(currentDate - 10000).substring(0, 4);
							month = "12";
							date = "32";
						}
						else if (month.equals("04") || month.equals("06") || month.equals("09") || month.equals("11")) {
							date = "31";
						}
						else if (month.equals("02")) {
							if (Integer.parseInt(year) % 4 == 0)
								date = "30";
							else
								date = "29";
						}
						else
							date = "32";
						currentDate = Integer.parseInt(year + month + date);
						//i ++;
					}
					/*else {
						previousDates.add(null);
						previousPrices.add(null);
					}*/
				}					
				else {	// throw the info into the lists
					previousDates.add(currentDate);
					previousPrices.add((double) map.get(currentDate));
					//maxSearchingDays = windowSize;
				}				
				
				currentDate --;
			}
			//return previousDates.toString();
			HashMap<Integer, ArrayList<Double>> movingAverageForEachDay = new HashMap<Integer, ArrayList<Double>>();
			movingAverageForEachDay.put(startDate, new ArrayList<Double>(previousPrices));	// prices in the past n days for the first date
			int updateIndex = previousPrices.size() - 1;	// index for updating the list
			for (int i = startDate + 1; i <= endDate; i ++) {
				currentDate = i - 1;
				if (String.valueOf(i).substring(6, 8).equals("32")) {	// deal with the situation where we cross months/years
					String year = String.valueOf(i).substring(0, 4);
					String month = String.valueOf(i + 100).substring(4, 6);
					String date = "01";
					if (month.equals("13")) {
						year = String.valueOf(i + 10000).substring(0, 4);
						month = "01";
					}
					currentDate = i;
					i = Integer.parseInt(year + month + date);
				}
				if (map.containsKey(currentDate)) {
					if (previousPrices.size() < windowSize) {
						previousDates.add(0, currentDate);
						previousPrices.add(0, (double) map.get(currentDate));
						updateIndex = previousPrices.size() - 1;
					}
					else {	// replace the oldest day with the new one
						previousDates.set(updateIndex, currentDate);
						previousPrices.set(updateIndex, (double) map.get(currentDate));
						updateIndex -= 1;
						if (updateIndex < 0) {
							updateIndex = previousPrices.size() - 1;
						}
					}
				}/*
				else {
					previousDates.remove(updateIndex);
					previousPrices.remove(updateIndex);
					previousDates.add(updateIndex, null);
					previousPrices.add(updateIndex, null);
					updateIndex --;
					if (updateIndex < 0) {
						updateIndex = previousPrices.size() - 1;
					}
				}*/
				movingAverageForEachDay.put(i, new ArrayList<Double>(previousPrices));	// throw the info for the date
			}
			DataBag output = mBagFactory.newDefaultBag();
			
			for (Entry<Integer, ArrayList<Double>> entry : movingAverageForEachDay.entrySet()) {
				//DataBag pricesBag = mBagFactory.newDefaultBag();
				
				Tuple dateTuple = mTupleFactory.newTuple();
				if (entry.getValue().isEmpty()) {
					dateTuple.append(entry.getKey());
					dateTuple.append(0);
					//dateTuple.append(pricesBag);
				}
				else {	// calculate the sum
					double sum = 0;
					int days = 0;
					for (Double price : entry.getValue()) {
						Tuple t = mTupleFactory.newTuple();
						if (price != null) {
							sum += price;
							days ++;
						}
						t.append(price);
						//pricesBag.add(t);
					}
					//	put date and average 
					dateTuple.append(entry.getKey());
					dateTuple.append(sum / days);
					//dateTuple.append(pricesBag);
				}
				output.add(dateTuple);	// output the result
			}
			//return movingAverageForEachDay.toString();
			return output;
		} catch (Exception e) {
			//Throwing an exception will cause the task to fail.
			throw new IOException(e);
		}
	}
	public Schema outputSchema(Schema input) {
		try {
			//Schema tupleSchema = new Schema();
			//tupleSchema.add(new FieldSchema("price", DataType.DOUBLE));

			//Schema bagSchema = new Schema(new FieldSchema("past_prices", tupleSchema));


			Schema dateTupleSchema = new Schema();
			dateTupleSchema.add(new FieldSchema("date", DataType.INTEGER));
			dateTupleSchema.add(new FieldSchema("average", DataType.DOUBLE));
			//dateTupleSchema.add(new FieldSchema("days_prices", bagSchema, DataType.BAG));
			
			Schema datesBagSchema = new Schema(new FieldSchema("dates_prices", dateTupleSchema, DataType.BAG));

			

			return datesBagSchema;
		} catch (Exception e) {
			return null;
		}
		
	}
}
