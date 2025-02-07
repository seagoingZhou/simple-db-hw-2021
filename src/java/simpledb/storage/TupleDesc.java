package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TDItem tdItem = (TDItem) o;
            return fieldType == tdItem.fieldType &&
                    Objects.equals(fieldName, tdItem.fieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldType, fieldName);
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private ArrayList<TDItem> tdItems;

    private int size = 0;

    public ArrayList<TDItem> getTdItems() {
        return tdItems;
    }

    private void setTdItems(ArrayList<TDItem> tdItems) {
        this.tdItems = tdItems;
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return getTdItems().iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if (typeAr == null || fieldAr == null || typeAr.length != fieldAr.length) {
            setTdItems(new ArrayList<>(0));
        } else {
            int num = typeAr.length;
            setTdItems(new ArrayList<>(num));
            for (int i = 0; i < num; ++i) {
                getTdItems().add(new TDItem(typeAr[i], fieldAr[i]));
                this.size += typeAr[i].getLen();
            }
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return getTdItems().size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= numFields()) {
            throw new NoSuchElementException();
        }
        return getTdItems().get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= numFields()) {
            throw new NoSuchElementException();
        }
        return getTdItems().get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        int index = 0;
        for (TDItem tdItem : getTdItems()) {
            if (Objects.equals(name, getFieldName(index))) {
                return index;
            }
            ++index;
        }

        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return this.size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int length = td1.numFields() + td2.numFields();
        Type[] fieldType = new Type[length];
        String[] fieldName = new String[length];

        int idx = 0;
        for (int i = 0; i < td1.numFields(); ++i) {
            fieldType[idx] = td1.getFieldType(i);
            fieldName[idx] = td1.getFieldName(i);
            ++idx;
        }

        for (int i = 0; i < td2.numFields(); ++i) {
            fieldType[idx] = td2.getFieldType(i);
            fieldName[idx] = td2.getFieldName(i);
            ++idx;
        }

        return new TupleDesc(fieldType, fieldName);
    }

    @Override
    public boolean equals(Object o) {
        return o == null ?
                this.tdItems == null :
                (o instanceof TupleDesc && this.tdItems.equals(((TupleDesc) o).getTdItems()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(tdItems);
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < numFields() - 1; ++i) {
            stringBuilder.append(getTdItems().get(i).toString() + ", ");
        }
        stringBuilder.append(getTdItems().get(numFields() - 1).toString());
        return stringBuilder.toString();
    }
}
