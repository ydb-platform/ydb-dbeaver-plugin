/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2026 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jkiss.dbeaver.ext.ydb.model.dashboard;

import java.util.Collections;
import java.util.List;

public class YDBDatabaseLoadInfo {

    private double coresUsed;
    private double coresTotal;
    private long storageUsed;
    private long storageTotal;
    private long memoryUsed;
    private long memoryTotal;
    private double networkBytesPerSec;
    private String overallStatus;
    private int nodesAlive;
    private int nodesTotal;
    private List<YDBNodeInfo> nodes;
    private int runningQueries;
    private String errorMessage;

    public YDBDatabaseLoadInfo() {
        this.nodes = Collections.emptyList();
    }

    public double getCoresUsed() {
        return coresUsed;
    }

    public void setCoresUsed(double coresUsed) {
        this.coresUsed = coresUsed;
    }

    public double getCoresTotal() {
        return coresTotal;
    }

    public void setCoresTotal(double coresTotal) {
        this.coresTotal = coresTotal;
    }

    public long getStorageUsed() {
        return storageUsed;
    }

    public void setStorageUsed(long storageUsed) {
        this.storageUsed = storageUsed;
    }

    public long getStorageTotal() {
        return storageTotal;
    }

    public void setStorageTotal(long storageTotal) {
        this.storageTotal = storageTotal;
    }

    public long getMemoryUsed() {
        return memoryUsed;
    }

    public void setMemoryUsed(long memoryUsed) {
        this.memoryUsed = memoryUsed;
    }

    public long getMemoryTotal() {
        return memoryTotal;
    }

    public void setMemoryTotal(long memoryTotal) {
        this.memoryTotal = memoryTotal;
    }

    public double getNetworkBytesPerSec() {
        return networkBytesPerSec;
    }

    public void setNetworkBytesPerSec(double networkBytesPerSec) {
        this.networkBytesPerSec = networkBytesPerSec;
    }

    public String getOverallStatus() {
        return overallStatus;
    }

    public void setOverallStatus(String overallStatus) {
        this.overallStatus = overallStatus;
    }

    public int getNodesAlive() {
        return nodesAlive;
    }

    public void setNodesAlive(int nodesAlive) {
        this.nodesAlive = nodesAlive;
    }

    public int getNodesTotal() {
        return nodesTotal;
    }

    public void setNodesTotal(int nodesTotal) {
        this.nodesTotal = nodesTotal;
    }

    public List<YDBNodeInfo> getNodes() {
        return nodes;
    }

    public void setNodes(List<YDBNodeInfo> nodes) {
        this.nodes = nodes != null ? nodes : Collections.emptyList();
    }

    public int getRunningQueries() {
        return runningQueries;
    }

    public void setRunningQueries(int runningQueries) {
        this.runningQueries = runningQueries;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public double getCpuPercent() {
        if (coresTotal <= 0) {
            return 0;
        }
        return coresUsed / coresTotal * 100;
    }

    public double getStorageUsedGB() {
        return storageUsed / (1024.0 * 1024.0 * 1024.0);
    }

    public double getStorageTotalGB() {
        return storageTotal / (1024.0 * 1024.0 * 1024.0);
    }

    public double getMemoryUsedGB() {
        return memoryUsed / (1024.0 * 1024.0 * 1024.0);
    }

    public double getMemoryTotalGB() {
        return memoryTotal / (1024.0 * 1024.0 * 1024.0);
    }

    public double getNetworkKBPerSec() {
        return networkBytesPerSec / 1024.0;
    }
}
