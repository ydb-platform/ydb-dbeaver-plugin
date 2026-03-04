# YDB DBeaver Plugin

[DBeaver](https://dbeaver.io/) extension with native support for [YDB (Yandex Database)](https://ydb.tech/).

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Build from source](#build-from-source)
- [Installation](#installation)
  - [Method 1: install from ZIP archive (P2 repository)](#method-1-install-from-zip-archive-p2-repository)
  - [Method 2: install from URL (recommended)](#method-2-install-from-url-recommended)
- [Creating a YDB connection](#creating-a-ydb-connection)
- [Authentication methods](#authentication-methods)
- [Object navigator](#object-navigator)
- [Plugin capabilities](#plugin-capabilities)
- [Updates](#updates)
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

Use this method to install from a locally built or downloaded archive.

**Step 1.** Open DBeaver. In the top menu select:

```
Help → Install New Software...
```

The Install dialog opens.

---

**Step 2.** Click **Add...** next to the "Work with:" field.

```
┌─────────────────────────────────────────────────────────┐
│ Install                                                 │
│                                                         │
│ Work with: [________________________] [Add...] [Manage] │
│                                                         │
│ type filter text                                        │
│ ┌─────────────────────────────────────────────────────┐ │
│ │ (empty — no repository selected)                    │ │
│ └─────────────────────────────────────────────────────┘ │
│                          [< Back] [Next >] [Cancel]     │
└─────────────────────────────────────────────────────────┘
```

---

**Step 3.** In the "Add Repository" dialog click **Archive...** and select the ZIP file:

```
┌─────────────────────────────────────────┐
│ Add Repository                          │
│                                         │
│ Name: [YDB Plugin                     ] │
│                                         │
│ Location: [jar:file:/path/to/...zip!/]  │
│           [Local...] [Archive...]       │
│                          [Add] [Cancel] │
└─────────────────────────────────────────┘
```

Select `org.jkiss.dbeaver.ext.ydb.repository-1.0.0-SNAPSHOT.zip`.

After clicking **Add**, DBeaver loads the archive contents.

---

**Step 4.** The category **DBeaver YDB Support** appears. Check it:

```
┌─────────────────────────────────────────────────────────┐
│ Work with: YDB Plugin - jar:file:/path/to/...zip!/      │
│                                                         │
│ ☑ DBeaver YDB Support                                   │
│   ☑ DBeaver YDB Support 1.0.0                           │
│                                                         │
│ ☐ Show only the latest versions of available software   │
│ ☑ Group items by category                               │
│                          [< Back] [Next >] [Cancel]     │
└─────────────────────────────────────────────────────────┘
```

Click **Next >**.

---

**Step 5.** On the "Install Details" screen verify both components are listed:

```
Items to install:
• org.jkiss.dbeaver.ext.ydb  1.0.0
• org.jkiss.dbeaver.ext.ydb.ui  1.0.0
```

Click **Next >**.

---

**Step 6.** DBeaver may show an unsigned content warning:

```
┌─────────────────────────────────────────────────────────────┐
│ Warning: Unsigned Content                                   │
│                                                             │
│ The following content is unsigned:                          │
│   - DBeaver YDB Support 1.0.0                               │
│                                                             │
│ If you proceed, you are putting the authenticity or         │
│ validity of this software at risk.                          │
│                                                             │
│                     [Install Anyway] [Cancel]               │
└─────────────────────────────────────────────────────────────┘
```

This is expected — the plugin JARs are not signed with a commercial certificate. Click **Install Anyway**.

> Eclipse (which DBeaver is based on) verifies JAR signatures to confirm authenticity. This open source plugin is distributed without a signature. The source code is available for review in this repository.

---

**Step 7.** Review the license (Apache License 2.0), select **I accept the terms of the license agreements**, and click **Finish**.

```
┌─────────────────────────────────────────────────────────┐
│ Review Licenses                                         │
│                                                         │
│ Licenses:                                               │
│ Apache License, Version 2.0                             │
│                                                         │
│ ○ I do not accept the terms of the license agreements   │
│ ● I accept the terms of the license agreements          │
│                          [< Back] [Finish] [Cancel]     │
└─────────────────────────────────────────────────────────┘
```

---

**Step 8.** DBeaver installs the plugin and prompts for a restart. Click **Restart Now**.

```
┌─────────────────────────┐
│ Software Updates        │
│                         │
│ A restart is required   │
│ to apply the software   │
│ updates. Restart now?   │
│                         │
│ [Restart Now] [Not Now] │
└─────────────────────────┘
```

After restart the plugin is active.

---

### Method 2: install from URL (recommended)

This method enables automatic updates.

**Steps 1–2.** Same as Method 1: open **Help → Install New Software...** and click **Add...**.

**Step 3.** In the "Add Repository" dialog enter the URL in the **Location** field:

```
https://storage.yandexcloud.net/ydb-dbeaver/dbeaver
```

```
┌─────────────────────────────────────────┐
│ Add Repository                          │
│                                         │
│ Name: [YDB Plugin                     ] │
│                                         │
│ Location: [https://storage.yandexcloud. │
│            net/ydb-dbeaver/dbeaver    ]  │
│           [Local...] [Archive...]       │
│                          [Add] [Cancel] │
└─────────────────────────────────────────┘
```

Click **Add**. DBeaver loads the repository metadata.

**Steps 4–8.** Follow steps 4–8 from Method 1 (select components, accept license, restart).

---

## Creating a YDB connection

**Step 1.** In the top menu select **Database → New Database Connection** (or press `Ctrl+Shift+N`).

---

**Step 2.** Type `YDB` in the search box. Select **YDB** from the list and click **Next**.

```
┌─────────────────────────────────────────────────────────┐
│ Connect to a database                                   │
│                                                         │
│ [YDB                                     ] ← search     │
│                                                         │
│ ┌─────────────────┐                                     │
│ │  [YDB logo]     │                                     │
│ │  YDB            │                                     │
│ └─────────────────┘                                     │
│                          [< Back] [Next >] [Cancel]     │
└─────────────────────────────────────────────────────────┘
```

---

**Step 3.** The YDB connection settings page opens:

```
┌─────────────────────────────────────────────────────────────┐
│ Connection Settings                                         │
│                                                             │
│ ┌ Connection ─────────────────────────────────────────────┐ │
│ │ Host:     [localhost           ]  Port: [2135]          │ │
│ │ Database: [/local              ]                        │ │
│ │ Monitoring URL: [              ]                        │ │
│ │ ☑ Use secure connection (grpcs://)                      │ │
│ │ ☑ Enable autocomplete API                               │ │
│ └─────────────────────────────────────────────────────────┘ │
│                                                             │
│ ┌ Authentication ─────────────────────────────────────────┐ │
│ │ Auth type: [Anonymous ▼]                                │ │
│ └─────────────────────────────────────────────────────────┘ │
│                                                             │
│ JDBC URL: jdbc:ydb:grpcs://localhost:2135/local            │
│                                                             │
│ [Test Connection]        [< Back] [Next >] [Finish]        │
└─────────────────────────────────────────────────────────────┘
```

Fill in the fields:

| Field | Description | Example |
|-------|-------------|---------|
| **Host** | YDB server host | `ydb.example.com` |
| **Port** | Port (default 2135) | `2135` |
| **Database** | Database path | `/ru-central1/b1gxxx/etn000` |
| **Monitoring URL** | YDB Viewer API URL for the dashboard (optional) | `http://ydb-viewer:8765` |
| **Use secure connection** | Enable TLS/SSL (`grpcs://`) | ☑ |
| **Enable autocomplete API** | Autocomplete via YDB API | ☑ |

---

**Step 4.** Select the authentication method from the **Auth type** dropdown (see [Authentication methods](#authentication-methods)).

---

**Step 5.** Click **Test Connection**. On success:

```
┌─────────────────────────┐
│ Connected (42 ms)       │
│              [OK]       │
└─────────────────────────┘
```

---

**Step 6.** Click **Finish**. The connection appears in the **Database Navigator** panel.

---

## Authentication methods

### Anonymous

Connect without credentials. Use for local or test YDB installations.

```
Auth type: [Anonymous ▼]
```

No additional fields.

---

### Static (username and password)

```
Auth type: [Static ▼]
┌────────────────────────────────┐
│ User:     [username          ] │
│ Password: [••••••••          ] │
└────────────────────────────────┘
```

Use when username/password authentication is enabled on the YDB server.

---

### Token

```
Auth type: [Token ▼]
┌────────────────────────────────────────────────┐
│ Token: [••••••••••••••••••••••••••••••••••••] │
└────────────────────────────────────────────────┘
```

Enter an IAM token or OAuth token. The token is sent in the header of every request.

---

### Service Account

```
Auth type: [Service Account ▼]
┌──────────────────────────────────────────────┐
│ SA Key File: [/path/to/key.json    ] [...]   │
└──────────────────────────────────────────────┘
```

Provide the path to a Yandex Cloud service account JSON key file. The `...` button opens a file picker.

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

```
Auth type: [Metadata ▼]
```

The plugin fetches an IAM token from the Yandex Cloud VM metadata service. Use only when DBeaver runs on a Yandex Cloud virtual machine.

---

## Object navigator

After connecting, the **Database Navigator** panel shows the YDB object hierarchy:

```
▼ YDB Connection
  ▼ /local
    ▼ Tables
      ▼ folder1
        ▷ subfolder
        📋 mytable
      📋 anothertable
    ▼ Topics
      📨 my-topic
    ▼ Views
      👁 my-view
    ▼ External Data Sources
      🔗 s3-source
    ▼ External Tables
      📋 ext-table
    ▼ System Views (.sys)
      📋 partition_stats
      📋 query_sessions
    ▼ Resource Pools
      ⚙ default
```

Tables are organized into folders according to their YDB path (e.g. table `/local/folder1/subfolder/mytable` appears nested under `folder1 → subfolder`).

---

## Plugin capabilities

### YQL editor

Open the **SQL Editor** (`F3` or double-click the connection). The editor supports:

- YQL syntax highlighting: keywords (`UPSERT`, `REPLACE`, `EVALUATE`, `PRAGMA`, `WINDOW` and 145+ more), data types, built-in functions
- Autocomplete for table names, columns, and functions
- Query execution: `Ctrl+Enter` — current query, `Ctrl+Shift+Enter` — entire script

```sql
-- Example YQL query
UPSERT INTO `/local/users` (id, name, created_at)
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

Right-click the connection and select **Manage Sessions**, or use **Database → Manage Sessions**.

```
┌──────────────────────────────────────────────────────────┐
│ Active Sessions                          [Hide Idle] ☑  │
├──────────┬──────────────────┬──────────────┬────────────┤
│ Session  │ Query            │ State        │ Duration   │
├──────────┼──────────────────┼──────────────┼────────────┤
│ abc123   │ SELECT * FROM …  │ Executing    │ 00:00:03   │
│ def456   │ —                │ Idle         │ 00:02:15   │
└──────────┴──────────────────┴──────────────┴────────────┘
```

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

Right-click an object (table, topic, folder, etc.) → **Edit Permissions**.

```
┌──────────────────────────────────────────────────────────┐
│ Permissions for /local/users                            │
├──────────────────────┬─────────────────────────────────┤
│ Subject              │ Permissions                     │
├──────────────────────┼─────────────────────────────────┤
│ user@example.com     │ SELECT, INSERT                  │
│ service-account@...  │ FULL                            │
└──────────────────────┴─────────────────────────────────┘
│ [Grant] [Revoke] [Set Owner]                           │
└──────────────────────────────────────────────────────────┘
```

---

### Streaming queries

In the navigator expand the **Streaming Queries** folder. For each query:

- View source (YQL)
- View issues
- View execution plan
- Actions: **Start**, **Stop**, **Alter**

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

If the plugin was installed via **Help → Install New Software → Add → URL** `https://storage.yandexcloud.net/ydb-dbeaver/dbeaver` (Method 2), DBeaver remembers that URL. Publishing a new repository at the same URL is enough:

Users receive the update:

1. Automatically on the next DBeaver start (if update checks are enabled — **Window → Preferences → Install/Update → Automatic Updates**)
2. Manually via **Help → Check for Updates**:

```
┌──────────────────────────────────────────────────────────────┐
│ Available Updates                                            │
│                                                              │
│ ☑ DBeaver YDB Support  1.0.0.v20260302-1652 → 1.0.0.v202604… │
│                                                              │
│                             [Select All] [Deselect All]      │
│                              [< Back] [Next >] [Cancel]      │
└──────────────────────────────────────────────────────────────┘
```

After selecting the update DBeaver follows the same steps as the first install (license → unsigned warning → restart).

### ZIP installation — automatic updates do **not** work

If the plugin was installed via **Archive...** (local ZIP file), DBeaver does not know where to look for updates. In that case:

1. Download the new ZIP archive
2. Remove the old version: **Help → About DBeaver → Installation Details → select plugin → Uninstall** → restart
3. Install the new version from ZIP following the same instructions as the first time

> **Recommendation:** install via URL `https://storage.yandexcloud.net/ydb-dbeaver/dbeaver` (Method 2) to receive updates automatically.

---

## License

[Apache License 2.0](LICENSE)
