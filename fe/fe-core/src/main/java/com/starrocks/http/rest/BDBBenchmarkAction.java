// This file is made available under Elastic License 2.0.

package com.starrocks.http.rest;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;
import com.starrocks.catalog.Catalog;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.journal.bdbje.CloseSafeDatabase;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BDBBenchmarkAction extends RestBaseAction {
    public static final Logger LOG = LogManager.getLogger(BDBBenchmarkAction.class);

    public AtomicLong key = new AtomicLong(0L);

    public int dbId = 0;

    public BDBBenchmarkAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/bdb_benchmark", new BDBBenchmarkAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        final long timeout = Long.parseLong(request.getSingleParameter("timeout")) * 1000L;
        int size = Integer.parseInt(request.getSingleParameter("msg_size"));
        int concurrency = Integer.parseInt(request.getSingleParameter("concurrency"));
        final byte[] data = new byte[size];
        final long startTime = System.currentTimeMillis();
        AtomicInteger cnt = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(concurrency);
        List<Long> latencyList = Collections.synchronizedList(new ArrayList<>());
        String dbName = "test" + (++dbId);
        CloseSafeDatabase db = openDatabase(dbName);
        for (int i = 0; i < concurrency; i++) {
            new Thread(() -> {
                try {
                    while (System.currentTimeMillis() - startTime < timeout) {
                        write(db, data);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        try {
            latch.await();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }

        removeDatabase(dbName);

        sendResult(request, response);
    }

    public CloseSafeDatabase openDatabase(String dbName) {
        BDBEnvironment env = ((BDBJEJournal) Catalog.getCurrentCatalog().getEditLog().getJournal()).getBdbEnvironment();
        return env.openDatabase(dbName);
    }

    public void removeDatabase(String dbName) {
        BDBEnvironment env = ((BDBJEJournal) Catalog.getCurrentCatalog().getEditLog().getJournal()).getBdbEnvironment();
        env.removeDatabase(dbName);
    }

    public boolean write(CloseSafeDatabase db, byte[] data) {
        Long id = key.getAndIncrement();
        DatabaseEntry theKey = new DatabaseEntry();
        TupleBinding<Long> idBinding = TupleBinding.getPrimitiveBinding(Long.class);
        idBinding.objectToEntry(id, theKey);

        DatabaseEntry theData = new DatabaseEntry(data);
        boolean success = false;
        for (int i = 0; i < 3; i++) {
            try {
                if (db.put(null, theKey, theData) == OperationStatus.SUCCESS) {
                    success = true;
                    break;
                } else {
                    LOG.warn("write bdb failed");
                }
            } catch (DatabaseException e) {
                LOG.warn("write bdb failed", e);
            }
        }
        return success;
    }
}
