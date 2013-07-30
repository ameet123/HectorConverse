import java.util.Iterator;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;

/**
 * an iterable class to loop through the columns of a single row
 * returns an iterator of HColumn<Composite,Double>
 * of course it can be changed to suit the desired return type
 * This could be wrapped inside a row iterator
 * @author ac2211
 */
public class CompositeColumnSliceIterator implements Iterable<HColumn<Composite,String>> {
	/**
	 * iterator variable to be returned by this class
	 */
	private final Iterator<?> columnIterator ;
	/**
	 * constructor accepts a row and iterates through its column via ColumnSlice getter
	 * @param row
	 */
	public CompositeColumnSliceIterator(Row<?,?,?> row) {
		columnIterator = row.getColumnSlice().getColumns().iterator();
		columnIterator.hasNext();
    }
	@SuppressWarnings("unchecked")
	@Override
	public Iterator<HColumn<Composite, String>> iterator() {
		return (Iterator<HColumn<Composite, String>>) columnIterator;
	}		
}