/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package txnIdSelfCheck;

import org.voltdb.ClientResponseImpl;
import org.voltdb.client.*;
import org.voltdb.VoltTable;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RowOrderTableLoader extends BenchmarkThread {

    //final boolean USE_SWAP_TABLES_SYSPROC = true;

    final Client client;
    final long targetCount;
    final String tableName;
    final String copyTableName;
    final String viewTableName;
    final int rowSize;
    final int batchSize;
    final Random r = new Random(0);
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final Semaphore m_permits;
    final int MAXROWS = 100000;
    String truncateProcedure = "TruncateTable";
    String swapProcedure = "SwapTables";
    String scanAggProcedure = "ScanAggTable";
    long insertsTried = 0;
    long rowsLoaded = 0;
    long nTruncates = 0;
    long nSwaps = 0;
    long nCopies = 0;
    float mpRatio;

    float swapRatio;

    RowOrderTableLoader(Client client, String tableName, String copyTableName, String viewTableName, long targetCount, int rowSize, int batchSize, Semaphore permits, float mpRatio, float swapRatio) {
        setName("RowOrderTableLoader");
        this.client = client;
        this.tableName = tableName;
        this.targetCount = targetCount;
        this.rowSize = rowSize;
        this.batchSize = batchSize;
        this.m_permits = permits;
        this.mpRatio = mpRatio;
        this.swapRatio = swapRatio;
        this.copyTableName = copyTableName;
        this.viewTableName = viewTableName;

        // make this run more than other threads
        log.info("RowOrderTableLoader table: "+ this.tableName + " targetCount: " + targetCount);

        // To get more detailed output, uncomment this:
        //log.setLevel(Level.DEBUG);
    }

    void shutdown() {
        m_shouldContinue.set(false);
        this.interrupt();
    }

    class InsertCallback implements ProcedureCallback {

        AtomicInteger latch;

        InsertCallback(AtomicInteger latch) {
            this.latch = latch;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            if (isStatusSuccess(clientResponse, (byte)0, "insert into", tableName)) {
                Benchmark.txnCount.incrementAndGet();
                rowsLoaded++;
            }
            latch.decrementAndGet();
        }
    }

    private boolean isStatusSuccess(ClientResponse clientResponse, String action, String table) {
           return isStatusSuccess(clientResponse,(byte)0, action, table);
    }
    private boolean isStatusSuccess(ClientResponse clientResponse,
            byte shouldRollback, String action, String table) {
        byte status = clientResponse.getStatus();
        if (status == ClientResponse.GRACEFUL_FAILURE ||
                (shouldRollback == 0 && status == ClientResponse.USER_ABORT)) {
            hardStop("RowOrderTableLoader gracefully failed to " + action + " table "
                + table + " and this shoudn't happen. Exiting.", clientResponse);
        }
        if (status == ClientResponse.SUCCESS) {
            return true;
        } else {
            // log what happened
            log.warn("RowOrderTableLoader ungracefully failed to " + action + " table " + table);
            log.warn(((ClientResponseImpl) clientResponse).toJSONString());
            return false;
        }
    }

    private long[] copyTables() throws IOException, ProcCallException {
        long[] b4RowCounts = new long[] {-1, -1};
        long[] afterRowCounts = new long[] {-1, -1};

        try {
            b4RowCounts[0] = TxnId2Utils.getRowCount(client, this.tableName);
            b4RowCounts[1]  = TxnId2Utils.getRowCount(client, this.copyTableName);
        } catch (Exception e) {
            hardStop("getrowcount exception", e);
        }

        ClientResponse clientResponse = null;

        clientResponse = TxnId2Utils.doProcCallOneShot(client, "@AdHoc",
                "INSERT INTO "+this.copyTableName + " SELECT * FROM "+this.tableName);

        Boolean result = isStatusSuccess(clientResponse, (byte) 1, "INSERT SELECT", this.tableName);

        /* check the table counts and various result cases
         */
        try {
            afterRowCounts[0] = TxnId2Utils.getRowCount(client, this.tableName);
            afterRowCounts[1] = TxnId2Utils.getRowCount(client, this.copyTableName);
        } catch (Exception e) {
            hardStop("getrowcount exception", e);
        }

        if (result) {
            if ( b4RowCounts[0]+b4RowCounts[1] != afterRowCounts[1] ) {
            // the best we can do is check that neither table has been mutated is some unexpected way
                String message = "INSERT SELECT on " + this.tableName + " to " + this.copyTableName
                        + " count(s) are not as expected result: before: "+this.tableName+ ":" + b4RowCounts[0] + ", "+this.copyTableName+":" + b4RowCounts[1]
                        + " after: "+this.tableName+":" + afterRowCounts[0] + ","+this.copyTableName+ ":" + afterRowCounts[1];
                hardStop("RowOrderTableLoader: " + message);
            }
            Benchmark.txnCount.incrementAndGet();
            nCopies++;

        } else {
            log.warn("INSERT SELECT client response failed, retrying ...");
        }
        return afterRowCounts;

    }

    private void truncateTable(String table) throws IOException, ProcCallException {
        ClientResponse clientResponse = null;

        log.info("truncating table "+table);
        clientResponse = client.callProcedure("@AdHoc", "truncate table "+table);
        if (isStatusSuccess(clientResponse, "truncate", table)) {
            Benchmark.txnCount.incrementAndGet();
            nTruncates++;
        } else {
            hardStop("RowOrderTableLoader truncate failed on table " + table );
        }


    }

    private void modifyRows() throws IOException, ProcCallException {
        ClientResponse clientResponse = null;

        clientResponse = client.callProcedure("@AdHoc", "select p from "+this.tableName +" limit 800" );
        if (! isStatusSuccess(clientResponse, "modifyrows", this.tableName)) {
            hardStop("RowOrderTableLoader gather modifier rows failed on table " + this.tableName );
        }

        VoltTable origTable = clientResponse.getResults()[0];
        while (origTable.advanceRow()) {
            long p = origTable.getLong(0);
            ClientResponse cr = client.callProcedure("@AdHoc", "update "+this.tableName+" set value='0000' where p = "+p );
            if ( ! isStatusSuccess(cr, "modifyrows", this.tableName)) {
                log.warn("RowOrderTableLoader modifyrows failed on table " + this.tableName );
            }
        }
    }

    // Compare this with a View and the copied table

    private void checkRowOrder() throws IOException, ProcCallException {
        ClientResponse clientResponse = null;

        clientResponse = client.callProcedure("@AdHoc", "select p from "+this.tableName );
        if (! isStatusSuccess(clientResponse, "select", this.tableName)) {
            hardStop("RowOrderTableLoader adhoc select failed on table " + this.tableName );
        }

        ClientResponse clientResponse2 = client.callProcedure("@AdHoc", "select p from "+this.copyTableName );
        if (! isStatusSuccess(clientResponse, "select", this.copyTableName)) {
            hardStop("RowOrderTableLoader adhoc select failed on table " + this.tableName );
        }

        ClientResponse clientResponse3 = client.callProcedure("@AdHoc", "select p from "+this.viewTableName );
        if (! isStatusSuccess(clientResponse, "select", this.viewTableName)) {
            hardStop("RowOrderTableLoader adhoc select failed on table " + this.tableName );
        }
        VoltTable origTable = clientResponse.getResults()[0];
        VoltTable copyTable = clientResponse2.getResults()[0];
        VoltTable viewTable = clientResponse3.getResults()[0];
        int i = 0;
        long origData = 0;
        long copyData = 0;
        long viewData = 0;
        long prevOrigData = 0;
        long prevCopyData = 0;
        long prevViewData = 0;
        while (origTable.advanceRow()) {
            origData = origTable.fetchRow(i).getLong(0);
            copyData = copyTable.fetchRow(i).getLong(0);
            viewData = viewTable.fetchRow(i).getLong(0);
            if ( origData != copyData ) {
                hardStop("RowOrderTableLoader copyTable are not in consistent order, got "+copyData+" expected:"+origData+" previous:" +prevCopyData );
            }
            if ( origData != viewData ) {
                hardStop("RowOrderTableLoader viewTable are not in consistent order , got "+viewData+" expected:"+origData+" previous:" +prevViewData );
            }
            prevOrigData = origData;
            prevCopyData = copyData;
            prevViewData = viewData;
            i++;
        }
        log.info("RowOrderTableLoader verified order of "+i+" rows");
    }

    @Override
    public void run() {
        byte[] data = new byte[rowSize];
        byte shouldRollback = 0;
        long currentRowCount = 0;
        long copyRowCount = 0;
        while (m_shouldContinue.get()) {
            r.nextBytes(data);


            try {
                currentRowCount = TxnId2Utils.getRowCount(client, this.tableName);
                if ( currentRowCount > MAXROWS ) truncateTable(this.tableName);
            } catch (ProcCallException e) {
                log.warn("RowOrderTableLoader failed ungraceful TruncateTable ProcCallException call for table '"
                        + this.tableName + "': " + e.getMessage());
            } catch (Exception e) {
                hardStop("getrowcount exception", e);
            }
            AtomicInteger latch = new AtomicInteger(0);
            try {
                // insert some batches...
                int tc = batchSize * r.nextInt(200);
                while ((currentRowCount < tc) && (m_shouldContinue.get())) {
                    latch = new AtomicInteger(0);
                    // try to insert batchSize random rows
                    for (int i = 0; i < batchSize; i++) {
                        long p = Math.abs(r.nextLong());
                        m_permits.acquire();
                        insertsTried++;
                        try {
                            client.callProcedure(new InsertCallback(latch), this.tableName.toUpperCase() + "TableInsert", p, data);
                            latch.incrementAndGet();
                        } catch (Exception e) {
                            // on exception, log and end the thread, but don't kill the process
                            log.error("RowOrderTableLoader failed a TableInsert procedure call for table '" + this.tableName + "': " + e.getMessage());
                            try {
                                Thread.sleep(3000);
                            } catch (Exception e2) {
                            }
                        }
                    }
                    while (latch.get() > 0)
                        Thread.sleep(10);
                    long nextRowCount = -1;
                    try {
                        nextRowCount = TxnId2Utils.getRowCount(client, this.tableName);
                    } catch (Exception e) {
                        hardStop("getrowcount exception", e);
                    }
                    // if no progress, throttle a bit
                    if (nextRowCount == currentRowCount) {
                        try { Thread.sleep(1000); } catch (Exception e2) {}
                    }
                    currentRowCount = nextRowCount;
                }
            } catch (Exception e) {
                hardStop(e);
            }

            if (latch.get() != 0) {
                hardStop("latch not zero " + latch.get());
            }

            // check the initial table counts, prior to truncate and/or swap
            try {
                currentRowCount = TxnId2Utils.getRowCount(client, this.tableName);
                copyRowCount = TxnId2Utils.getRowCount(client, this.copyTableName);
            } catch (Exception e) {
                hardStop("getrowcount exception", e);
            }

            try {
                log.debug(this.tableName + " current row count is " + currentRowCount
                         + "; " + this.copyTableName + " row count is " + copyRowCount);

                // copy the tables using a nondeterministic "insert select" without an order by

                truncateTable(this.copyTableName);
                long[] rowCounts = copyTables();
                currentRowCount = rowCounts[0];
                copyRowCount = rowCounts[1];

                modifyRows();

                checkRowOrder();
            }
            catch (ProcCallException e) {
                ClientResponseImpl cri = (ClientResponseImpl) e.getClientResponse();
                // this implies bad data and is fatal
                if ((cri.getStatus() == ClientResponse.GRACEFUL_FAILURE) ||
                    (cri.getStatus() == ClientResponse.USER_ABORT)) {
                        // on exception, log and end the thread, but don't kill the process
                    log.warn("RowOrderTableLoader failed  status:"+cri.getStatus()+" modifyrows ProcCallException call for table '"
                                + this.tableName + "': " + e.getMessage());
                } else {
                    log.warn("RowOrderTableLoader failed status:"+cri.getStatus()+" ungraceful modifyrows ProcCallException call for table '"
                        + this.tableName + "': " + e.getMessage());
                }
            }
            catch (NoConnectionsException e) {
                // on exception, log and end the thread, but don't kill the process
                log.warn("RowOrderTableLoader failed a non-proc call exception for table '" + this.tableName + "': " + e.getMessage());
                try { Thread.sleep(3000); } catch (Exception e2) { break;}
            }
            catch (IOException e) {
                // just need to fall through and get out
                throw new RuntimeException(e);
            }


            log.info("RowOrderTableLoader compared successfully. table: " + this.tableName + "; rows sent: " + insertsTried + "; inserted: "
                    + rowsLoaded + "; truncates: " + nTruncates + "; copies: " + nCopies);

            try { Thread.sleep(5000); } catch (Exception e2) { break;}
        }
        log.info("RowOrderTableLoader normal exit for table " + this.tableName + "; rows sent: " + insertsTried
                + "; inserted: " + rowsLoaded + "; truncates: " + nTruncates + "; copies: " + nCopies);
    }

}
