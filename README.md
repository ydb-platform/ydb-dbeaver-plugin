# YDB DBeaver Plugin

[DBeaver](https://dbeaver.io/) extension with native support for [YDB (Yandex Database)](https://ydb.tech/).

## Table of Contents

- [YDB DBeaver Plugin](#ydb-dbeaver-plugin)
  - [Table of Contents](#table-of-contents)
  - [Features](#features)
  - [Requirements](#requirements)
  - [Build from source](#build-from-source)
  - [Installation](#installation)
    - [Method 1: install from ZIP archive (P2 repository)](#method-1-install-from-zip-archive-p2-repository)
    - [Method 2: install from URL (recommended)](#method-2-install-from-url-recommended)
  - [Creating a YDB connection](#creating-a-ydb-connection)
  - [Authentication methods](#authentication-methods)
    - [Anonymous](#anonymous)
    - [Static (username and password)](#static-username-and-password)
    - [Token](#token)
    - [Service Account](#service-account)
    - [Metadata](#metadata)
  - [Object navigator](#object-navigator)
  - [Plugin capabilities](#plugin-capabilities)
    - [YQL editor](#yql-editor)
    - [EXPLAIN and execution plan](#explain-and-execution-plan)
    - [Session manager](#session-manager)
    - [Cluster dashboard](#cluster-dashboard)
    - [Access rights (ACL)](#access-rights-acl)
    - [Streaming queries](#streaming-queries)
    - [Creating objects](#creating-objects)
  - [Updates](#updates)
    - [URL installation — automatic updates work](#url-installation--automatic-updates-work)
    - [ZIP installation — automatic updates do **not** work](#zip-installation--automatic-updates-do-not-work)
  - [License](#license)

---

## Features

- Connect to YDB with all authentication methods (anonymous, static, token, service account, metadata)
- Hierarchical object navigator: tables, topics, external data sources, external tables, views
- System objects: `.sys`, Resource Pools, Resource Pool Classifiers
- YQL editor with 150+ keywords and built-in functions highlighted
- Execution plan visualization (EXPLAIN / EXPLAIN ANALYZE)
- Active session monitor
- Cluster dashboard: CPU, storage, memory, network, node status (refreshes every 5 seconds)
- Access rights management (ACL): grant, revoke, view permissions
- Topic message viewer (YDB Topics / PersQueue)
- Streaming query management: view, alter, start, stop
- Federated queries via external data sources (S3, databases)
- Specialized editors for JSON, JSONDOCUMENT, YSON data types

---

## Requirements

| Component | Version         |
|-----------|-----------------|
| DBeaver   | CE 24.x or later |
| Java      | 21+             |
| Maven     | 3.9+ (build only) |

---

## Build from source

```bash
git clone https://github.com/ydb-platform/ydb-dbeaver-plugin.git
cd ydb-dbeaver-plugin
mvn clean package -DskipTests
```

After the build, the P2 repository ZIP will be at:

```
repository/target/org.jkiss.dbeaver.ext.ydb.repository-1.0.0-SNAPSHOT.zip
```

To build and run tests:

```bash
mvn clean verify
```

---

## Installation

### Method 1: install from ZIP archive (P2 repository)

Download the latest `ydb-dbeaver-plugin-*.zip` from the [GitHub Releases](https://github.com/ydb-platform/ydb-dbeaver-plugin/releases) page.

This archive works offline — the JDBC driver is bundled inside and no internet connection is required during installation.

**Step 1.** Open DBeaver. In the top menu select **Help → Install New Software...**

**Step 2.** Click **Add...** next to the "Work with:" field.

**Step 3.** In the "Add Repository" dialog click **Archive...**, select the downloaded ZIP file, enter a name (e.g. `YDB Plugin`), and click **Add**. DBeaver loads the archive contents.

**Step 4.** The category **DBeaver YDB Support** appears in the list. Check it and click **Next >**.

**Step 5.** On the "Install Details" screen verify that both components are listed (`org.jkiss.dbeaver.ext.ydb` and `org.jkiss.dbeaver.ext.ydb.ui`) and click **Next >**.

**Step 6.** DBeaver may show an unsigned content warning. This is expected — the plugin JARs are not signed with a commercial certificate. Click **Install Anyway**.

> Eclipse (which DBeaver is based on) verifies JAR signatures to confirm authenticity. This open source plugin is distributed without a signature. The source code is available for review in this repository.

**Step 7.** Review the license (Apache License 2.0), select **I accept the terms of the license agreements**, and click **Finish**.

**Step 8.** DBeaver installs the plugin and prompts for a restart. Click **Restart Now**. After restart the plugin is active.

---

### Method 2: install from URL (recommended)

This method enables automatic updates.

**Steps 1–2.** Same as Method 1: open **Help → Install New Software...** and click **Add...**.

**Step 3.** In the "Add Repository" dialog enter a name (e.g. `YDB Plugin`) and paste the following URL into the **Location** field:

```
https://storage.yandexcloud.net/ydb-dbeaver-plugin
```

Click **Add**. DBeaver loads the repository metadata.

**Steps 4–8.** Follow steps 4–8 from Method 1 (select components, accept license, restart).

---

## Creating a YDB connection

**Step 1.** In the top menu select **Database → New Database Connection** (or press `Ctrl+Shift+N`).

**Step 2.** Type `YDB` in the search box. Select **YDB** from the list and click **Next**.

**Step 3.** The YDB connection settings page opens. Fill in the fields:

| Field | Description | Example |
|-------|-------------|---------|
| **Host** | YDB server host | `ydb.example.com` |
| **Port** | Port (default 2135) | `2135` |
| **Database** | Database path | `/Root/database` |
| **Monitoring URL** | YDB Viewer API URL for the dashboard (optional) | `http://ydb.example.com:8765` |
| **Use secure connection** | Enable TLS/SSL (`grpcs://`) | ☑ |
| **Enable autocomplete API** | Autocomplete via YDB API | ☑ |

**Step 4.** Select the authentication method from the **Auth type** dropdown (see [Authentication methods](#authentication-methods)).

**Step 5.** Click **Test Connection**. On success a dialog appears showing the connection time in milliseconds.

**Step 6.** Click **Finish**. The connection appears in the **Database Navigator** panel.

---

## Authentication methods

### Anonymous

Connect without credentials. Use for local or test YDB installations. Select **Anonymous** from the **Auth type** dropdown. No additional fields are required.

---

### Static (username and password)

Select **Static** from the **Auth type** dropdown. Enter the username in the **User** field and the password in the **Password** field. Use when username/password authentication is enabled on the YDB server.

---

### Token

Select **Token** from the **Auth type** dropdown. Enter an IAM token or OAuth token in the **Token** field. The token is sent in the header of every request.

---

### Service Account

Select **Service Account** from the **Auth type** dropdown. Provide the path to a Yandex Cloud [service account](https://yandex.cloud/en/docs/iam/concepts/users/service-accounts) JSON key file in the **SA Key File** field (use the **...** button to open a file picker). See [how to create an authorized key](https://yandex.cloud/en/docs/iam/operations/authentication/manage-authorized-keys) in the Yandex Cloud documentation.

The key file format:
```json
{
  "id": "aje...",
  "service_account_id": "aje...",
  "private_key": "-----BEGIN RSA PRIVATE KEY-----\n..."
}
```

---

### Metadata

Select **Metadata** from the **Auth type** dropdown. The plugin fetches an IAM token from the [Yandex Cloud VM metadata service](https://yandex.cloud/en/docs/compute/operations/vm-metadata/get-vm-metadata). Use only when DBeaver runs on a Yandex Cloud virtual machine.

---

## Object navigator

After connecting, the **Database Navigator** panel shows the YDB object hierarchy. The root node is the connection, inside it is the database path, which contains the following folders:

- **Tables** — organized into subfolders according to the YDB path (e.g. a table at `folder1/subfolder/mytable` appears nested under `folder1 → subfolder`)
- **Topics**
- **Views**
- **External Data Sources**
- **External Tables**
- **System Views (.sys)** — system views such as `partition_stats`, `query_sessions`
- **Resource Pools**

---

## Plugin capabilities

### YQL editor

Open the **SQL Editor** (`F3` or double-click the connection). The editor supports:

- YQL syntax highlighting: keywords (`UPSERT`, `REPLACE`, `EVALUATE`, `PRAGMA`, `WINDOW` and 145+ more), data types, built-in functions
- Autocomplete for table names, columns, and functions
- Query execution: `Ctrl+Enter` — current query, `Ctrl+Shift+Enter` — entire script

```sql
-- Example YQL query
UPSERT INTO `users` (id, name, created_at)
VALUES (1, "Alice", CurrentUtcDatetime());
```

---

### EXPLAIN and execution plan

Click **Explain** (or `Ctrl+Shift+E`) to get the execution plan. The plugin shows:

- **Text plan** — operation tree
- **Diagram** — graphical DAG representation
- **SVG plan** — interactive visualization

`EXPLAIN ANALYZE` additionally shows execution statistics (row counts, elapsed time).

---

### Session manager

Right-click the connection and select **Manage Sessions**, or use **Database → Manage Sessions**. The view lists all active sessions with their current query, state, and duration. The **Hide Idle** checkbox filters out sessions that have no active query.

---

### Cluster dashboard

Open the **Dashboard** tab in the connection editor (requires the **Monitoring URL** field to be set during setup).

The dashboard shows in real time (refreshes every 5 seconds):
- CPU load per node
- Disk space usage
- Memory usage
- Network traffic
- Number of running queries
- Cluster node status

---

### Access rights (ACL)

Right-click an object (table, topic, folder, etc.) → **Edit Permissions**. The dialog lists all subjects (users, service accounts) and their permissions. Use the **Grant**, **Revoke**, and **Set Owner** buttons to manage access.

---

### Streaming queries

In the navigator expand the **Streaming Queries** folder. For each query you can:

- View source (YQL)
- View issues
- View execution plan
- Perform actions: **Start**, **Stop**, **Alter**

---

### Creating objects

Right-click a folder or object → **Create New**:

- **Create Table** — create a new table
- **Create Topic** — create a new topic
- **Create Resource Pool** — create a resource pool

---

## Updates

DBeaver uses the Eclipse P2 mechanism to detect and install updates. When the plugin is installed, DBeaver remembers the repository URL source. When a new version is published, DBeaver compares the installed version with the one in the repository.

Each new build automatically gets a unique version like `1.0.0.v20260302-1652` (build date and time), so after publishing a new archive users will see the update without any extra steps on your side.

### URL installation — automatic updates work

If the plugin was installed via **Help → Install New Software → Add → URL** `https://storage.yandexcloud.net/ydb-dbeaver-plugin` (Method 2), DBeaver remembers that URL. Publishing a new repository at the same URL is enough.

Users receive the update:

1. Automatically on the next DBeaver start (if update checks are enabled — **Window → Preferences → Install/Update → Automatic Updates**)
2. Manually via **Help → Check for Updates**: select the available update and follow the same steps as the first install (license → unsigned warning → restart)

### ZIP installation — automatic updates do **not** work

If the plugin was installed via **Archive...** (local ZIP file), DBeaver does not know where to look for updates. In that case:

1. Download the new ZIP archive
2. Remove the old version: **Help → About DBeaver → Installation Details → select plugin → Uninstall** → restart
3. Install the new version from ZIP following the same instructions as the first time

> **Recommendation:** install via URL `https://storage.yandexcloud.net/ydb-dbeaver-plugin` (Method 2) to receive updates automatically.

---

## License

[Apache License 2.0](LICENSE)
