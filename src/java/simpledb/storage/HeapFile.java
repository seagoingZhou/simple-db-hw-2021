package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return getFile().getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        int pageNum = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        byte[] data = HeapPage.createEmptyPageData();
        try{
            FileInputStream fileInputStream = new FileInputStream(getFile());
            fileInputStream.skip(pageNum * pageSize);
            fileInputStream.read(data);
            return new HeapPage(new HeapPageId(tableId, pageNum), data);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) this.file.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    protected class HeapFileIterator implements DbFileIterator {

        private final TransactionId transactionId;
        private Iterator<Tuple> tupleIter;
        private int pageCursor;

        public HeapFileIterator(TransactionId transactionId) {
            this.transactionId = transactionId;
            this.tupleIter = null;
            this.pageCursor = -1;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            initPageCursor();
            setCurPageIterator(getPageCursor());
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen()) {
                return false;
            }
            while (getPageCursor() < numPages() - 1) {
                if (getTupleIter().hasNext()) {
                    return true;
                } else {
                    setCurPageIterator(incPageCursor());
                }
            }
            return getTupleIter().hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext()) {
                return getTupleIter().next();
            }
            throw new NoSuchElementException();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            this.pageCursor = -1;
            setTupleIter(null);
        }

        private void setCurPageIterator(int curPageNumber) throws DbException, TransactionAbortedException {
            if (curPageNumber > numPages()) {
                throw new DbException("");
            }
            HeapPageId pageId = new HeapPageId(getId(), curPageNumber);
            HeapPage page = (HeapPage) Database.getBufferPool()
                    .getPage(this.transactionId, pageId, Permissions.READ_ONLY);
            setTupleIter(page.iterator()) ;
        }

        private Iterator<Tuple> getTupleIter() {
            return this.tupleIter;
        }

        private void setTupleIter(Iterator<Tuple> tupleIter) {
            this.tupleIter = tupleIter;
        }

        private int getPageCursor() {
            return this.pageCursor;
        }

        private void initPageCursor() {
            this.pageCursor = 0;
        }

        private int incPageCursor() {
            return ++this.pageCursor;
        }

        private boolean isOpen() {
            return this.pageCursor > -1;
        }
    }


    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

