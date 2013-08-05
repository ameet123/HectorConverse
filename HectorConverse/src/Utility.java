import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.eaio.uuid.UUIDGen;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;


public class Utility {
	static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
	/**
	 * static method to execute range slice query, the limit on columns is set in the initial query
	 * the latter piece - column iterator simply iterates over that limit and returns the results
	 * @param ks
	 * @param columnFamily
	 * @param startKey
	 * @param endKey
	 * @param limit
	 * @return
	 */
	public static Rows<?,?,?> getCompositeRangeRowIterator(Keyspace ks, String columnFamily, String startKey, String endKey, 
																Composite startCol, Composite endCol, int limit)   {		        
		// first construct the slice range query
		RangeSlicesQuery<String, Composite, String> rangeSlicesQuery =
	            HFactory.createRangeSlicesQuery(ks, StringSerializer.get(), new CompositeSerializer(), StringSerializer.get());
		rangeSlicesQuery.setColumnFamily(columnFamily);      
        rangeSlicesQuery.setKeys(startKey,endKey);
        rangeSlicesQuery.setRange(startCol, endCol, false, limit);
        // now execute and get results
        QueryResult<OrderedRows<String, Composite, String>> results = rangeSlicesQuery.execute();
        // now get the multiple rows from this result set
        Rows<?, ?, ?> rows = (Rows<?, ?, ?>) results.get();
        return rows;
	}
	/**
	 * slice query with a limit on # of columns
	 * @param ks
	 * @param columnFamily
	 * @param key
	 * @param startCol
	 * @param endCol
	 * @param limit
	 * @return QueryReslut<ColumnSlice<Composite, String>>
	 */
	public static QueryResult<ColumnSlice<Composite, String>> getLimitSliceQuery
					(Keyspace ks, String columnFamily, String key, Composite startCol, Composite endCol, int limit){
		SliceQuery<String, Composite, String> sliceQuery =
    			HFactory.createSliceQuery(ks, StringSerializer.get(), new CompositeSerializer(), StringSerializer.get());
    	sliceQuery.setColumnFamily(columnFamily);
    	sliceQuery.setKey(key);
    	sliceQuery.setRange(startCol, endCol, false, limit);
    	QueryResult<ColumnSlice<Composite, String>> results = sliceQuery.execute();
    	return results;
	}
	/**
	 * form composite column to be used for start and end
	 * @param equalityOp
	 * @return Composite
	 */
	public static Composite compositeFrom(String componentName, Composite.ComponentEquality equalityOp){
		Composite composite = new Composite();

		composite.addComponent(componentName, StringSerializer.get(), "UTF8Type", equalityOp);
		return composite;
		
	}
	/**
	 * create a composite column name based on a TimeUUID type as its first element
	 * The TimeUUID element is generated based on a date string passed
	 * the format of the date string is "yyyy-MM-dd HH:mm:ss"
	 * Cassandra can only apply an INEQUALITY operator on the OUTERMOST element
	 * and NOT on the first element. The first element needs to be an EQUAL Operator
	 * If cassandra does not find a value matching the value passed in case of 
	 * an EQUAL Operator, then it picks the next one in the ordered sequence of columns
	 * @param dateString
	 * @param equalityOp
	 * @return Composite
	 */
	public static Composite UUIDfromDateString(String dateString, Composite.ComponentEquality equalityOp){
		// first convert date string to date
		Date date = null;
		try {
			date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateString);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long time = date.getTime();
		UUID u1 = TimeUUIDUtils.getTimeUUID(time);
		
		// now create the composite column
		Composite composite = new Composite();	
		composite.addComponent(0, u1,  equalityOp);		
		return composite;
	}
	public static java.util.UUID getUniqueTimeUUIDinMillis(long time) {
	    return new java.util.UUID(time, UUIDGen.getClockSeqAndNode());
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	/**
	 * a convenience, generic method to get appropriate serializer for column types
	 * Based on the list created and passed which contains string values 
	 * referring to the classes to be used for serializing values,
	 * this method calls the "get" method on them after getting their class.
	 * @param i
	 * @param typeList
	 * @return
	 */
	public static Serializer<?> getSerializer(int i, List<String> typeList) {
		@SuppressWarnings({ })
		Class<AbstractSerializer> serialzerClass = null;
		try {
			serialzerClass = (Class<AbstractSerializer>) Class.forName(typeList.get(i));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Method m = null;
		try {
			m = serialzerClass.getMethod("get");
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Object returnSerializer = null;
		try {
			returnSerializer = m.invoke(null, new Object[0]);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return (Serializer<?>) returnSerializer;		
	}
}