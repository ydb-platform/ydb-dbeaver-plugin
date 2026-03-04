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

public class YDBNodeInfo {

    private final int nodeId;
    private final String host;
    private final String version;
    private final double loadAverage;
    private final int numberOfCpus;
    private final long memoryUsed;
    private final long memoryLimit;

    public YDBNodeInfo(int nodeId, String host, String version, double loadAverage, int numberOfCpus, long memoryUsed, long memoryLimit) {
        this.nodeId = nodeId;
        this.host = host;
        this.version = version;
        this.loadAverage = loadAverage;
        this.numberOfCpus = numberOfCpus;
        this.memoryUsed = memoryUsed;
        this.memoryLimit = memoryLimit;
    }

    public int getNodeId() {
        return nodeId;
    }

    public String getHost() {
        return host;
    }

    public String getVersion() {
        return version;
    }

    public double getLoadAverage() {
        return loadAverage;
    }

    public int getNumberOfCpus() {
        return numberOfCpus;
    }

    public long getMemoryUsed() {
        return memoryUsed;
    }

    public long getMemoryLimit() {
        return memoryLimit;
    }

    public double getLoadPercent() {
        if (numberOfCpus <= 0) {
            return 0;
        }
        return loadAverage / numberOfCpus * 100;
    }
}
