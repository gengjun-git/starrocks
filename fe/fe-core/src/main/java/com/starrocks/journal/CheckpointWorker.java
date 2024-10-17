// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.journal;

import com.google.common.base.Preconditions;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ImageLoader;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.staros.StarMgrServer;
import io.trino.hive.$internal.com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class CheckpointWorker extends FrontendDaemon {
    public static final Logger LOG = LogManager.getLogger(CheckpointWorker.class);

    private final String imageDir;
    private final Journal journal;
    private final boolean belongToGlobalStateMgr;

    private NextPoint nextPoint;

    public CheckpointWorker(String name, Journal journal, String subDir) {
        super(name, FeConstants.checkpoint_interval_second * 1000L);
        this.imageDir = GlobalStateMgr.getServingState().getImageDir() + subDir;
        this.journal = journal;
        this.belongToGlobalStateMgr = Strings.isNullOrEmpty(subDir);
    }

    public void setNextCheckpoint(long epoch, long journalId) throws Exception {
        if (epoch != GlobalStateMgr.getCurrentState().getEpoch()) {
            throw new Exception(String.format("epoch: %d is not equal to current epoch: %d",
                    epoch, GlobalStateMgr.getCurrentState().getEpoch()));
        }
        if (journalId > GlobalStateMgr.getCurrentState().getMaxJournalId()) {
            throw new Exception(String.format("can not find journal id: %d , current max journal id is: %d",
                    journalId, GlobalStateMgr.getCurrentState().getMaxJournalId()));
        }

        nextPoint = new NextPoint(epoch, journalId);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (nextPoint == null) {
            return;
        }
        if (nextPoint.journalId <= getImageJournalId()) {
            return;
        }
        if (nextPoint.epoch != GlobalStateMgr.getCurrentState().getEpoch()) {
            return;
        }

        createImage(nextPoint.epoch, nextPoint.journalId);
    }

    private void finishCheckpoint(boolean isSuccess, String reason) {

    }

    private long getImageJournalId() {
        try {
            ImageLoader imageLoader = new ImageLoader(imageDir);
            return imageLoader.getImageJournalId();
        } catch (IOException e) {
            LOG.warn("get image journal id failed", e);
            return 0;
        }
    }

    private void createImage(long epoch, long journalId) {
        try {
            if (belongToGlobalStateMgr) {
                replayAndGenerateGlobalStateMgrImage(epoch, journalId);
            } else {
                replayAndGenerateStarMgrImage(epoch, journalId);
            }
        } catch (Exception e) {
            LOG.warn("create image failed", e);
            finishCheckpoint(false, e.getMessage());
            return;
        }

        finishCheckpoint(true, "success");
    }

    private void replayAndGenerateGlobalStateMgrImage(long epoch, long journalId) throws Exception {
        Preconditions.checkState(belongToGlobalStateMgr,
                "generate global state mgr checkpoint, but belongToGlobalStateMgr is false");
        long replayedJournalId = -1;
        // generate new image file
        LOG.info("begin to generate new image: image.{}", journalId);
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        globalStateMgr.setEditLog(new EditLog(null));
        globalStateMgr.setJournal(journal);
        try {
            globalStateMgr.loadImage(imageDir);
            globalStateMgr.initDefaultWarehouse();

            checkEpoch(epoch);

            globalStateMgr.replayJournal(journalId);
            globalStateMgr.clearExpiredJobs();

            checkEpoch(epoch);

            globalStateMgr.saveImage();
            replayedJournalId = globalStateMgr.getReplayedJournalId();
            if (MetricRepo.hasInit) {
                MetricRepo.COUNTER_IMAGE_WRITE.increase(1L);
            }
            GlobalStateMgr.getServingState().setImageJournalId(journalId);
            LOG.info("checkpoint finished save image.{}", replayedJournalId);
        } finally {
            GlobalStateMgr.destroyCheckpoint();
        }
    }

    private void replayAndGenerateStarMgrImage(long epoch, long journalId) throws Exception {
        Preconditions.checkState(!belongToGlobalStateMgr,
                "generate star mgr checkpoint, but belongToGlobalStateMgr is true");
        StarMgrServer starMgrServer = StarMgrServer.getCurrentState();
        try {
            starMgrServer.replayAndGenerateImage(imageDir, journalId);
        } finally {
            // destroy checkpoint, reclaim memory
            StarMgrServer.destroyCheckpoint();
        }
    }

    private void checkEpoch(long epoch) throws Exception {
        if (epoch != GlobalStateMgr.getServingState().getEpoch()) {
            throw new Exception("epoch outdated");
        }
    }

    static class NextPoint {
        private final long epoch;
        private final long journalId;

        public NextPoint(long epoch, long journalId) {
            this.epoch = epoch;
            this.journalId = journalId;
        }
    }
}
