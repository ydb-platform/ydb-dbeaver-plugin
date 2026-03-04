# Contributing to YDB DBeaver Plugin

## Requirements

- Java 21+
- Maven 3.9+
- DBeaver CE source (optional, for IDE integration)

## Build

```bash
git clone https://github.com/ydb-platform/ydb-dbeaver-plugin.git
cd ydb-dbeaver-plugin
mvn clean package -DskipTests
```

The installable P2 repository ZIP will be at:
```
repository/target/org.jkiss.dbeaver.ext.ydb.repository-1.0.0-SNAPSHOT.zip
```

## Run tests

```bash
mvn clean verify
```

Tests require a graphical environment (Eclipse/SWT). On headless CI, Xvfb is needed:

```bash
Xvfb :99 &
DISPLAY=:99 mvn clean verify
```

## Project structure

```
plugins/org.jkiss.dbeaver.ext.ydb/      # Core: model, gRPC, metadata, plan, session, dashboard
plugins/org.jkiss.dbeaver.ext.ydb.ui/   # UI: editors, dialogs, handlers, renderers
features/                                # OSGi feature packaging
repository/                              # P2 update site
tests/                                   # Unit tests
```

The core plugin bundles `ydb-jdbc-driver-shaded` — no separate driver installation is needed.

## Key dependencies

- [YDB JDBC Driver](https://github.com/ydb-platform/ydb-jdbc-driver) — bundled, downloaded by Maven at build time
- [YDB Java SDK](https://github.com/ydb-platform/ydb-java-sdk) — used via the shaded JDBC driver JAR
- DBeaver CE — provided by P2 repository at build time

## Making changes

- Core model classes live in `plugins/org.jkiss.dbeaver.ext.ydb/src/org/jkiss/dbeaver/ext/ydb/model/`
- UI handlers and editors live in `plugins/org.jkiss.dbeaver.ext.ydb.ui/src/org/jkiss/dbeaver/ext/ydb/ui/editors/`
- New object types must be registered in `plugin.xml` of the respective plugin
- UI labels go into `OSGI-INF/l10n/bundle.properties`

Write tests for any changed functionality. Test classes live in `tests/org.jkiss.dbeaver.ext.ydb.tests/`.

## Submitting a PR

1. Fork the repository and create a branch from `main`
2. Make your changes and ensure `mvn clean verify` passes
3. Open a pull request with a description of what was changed and why

## Reporting issues

Please use [GitHub Issues](https://github.com/ydb-platform/ydb-dbeaver-plugin/issues).
When reporting a bug, include the DBeaver version, Java version, and steps to reproduce.
