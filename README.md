Trino git Connector
===================

[![Build Status](https://github.com/nineinchnick/trino-git/actions/workflows/release.yaml/badge.svg)](https://github.com/nineinchnick/trino-git/actions/workflows/release.yaml)

This is a [Trino](https://trino.io/) connector to access git repos. Please keep in mind that this is not production ready and it was created for tests.

# Quick Start

To run a Docker container with the connector and the latest release, run the following:

```bash
VERSION=$(curl -fsSL https://api.github.com/repos/nineinchnick/trino-git/releases/latest | sed -n 's/.*"tag_name": *"v\([^"]*\)".*/\1/p')
docker run \
  -d \
  --name trino-git \
  -e REPO_URL=https://github.com/nineinchnick/trino-rest.git \
  -p 8080:8080 \
  nineinchnick/trino-git:$VERSION
```

The image has a `git` catalog that reads the repo from `REPO_URL`.
Then connect to it with the Trino CLI that is included in the image:

```bash
docker exec -it trino-git trino --catalog git --schema default
```

You can also use any other SQL client to connect to Trino running at http://localhost:8080.

# Usage

Download the ZIP package of the [latest release](https://github.com/nineinchnick/trino-git/releases/latest),
unzip it, and copy the `trino-git-<version>` directory as `plugin/git` on every node in your Trino cluster:

```bash
VERSION=$(curl -fsSL https://api.github.com/repos/nineinchnick/trino-git/releases/latest | sed -n 's/.*"tag_name": *"v\([^"]*\)".*/\1/p')
curl -fsSLO https://github.com/nineinchnick/trino-git/releases/download/v$VERSION/trino-git-$VERSION.zip
unzip trino-git-$VERSION.zip
cp -r trino-git-$VERSION ${TRINO_HOME}/plugin/git
```

Create a catalog properties file, for example `etc/catalog/git.properties`, that points to a remote repo.
The catalog name is the file name, so this file creates the `git` catalog.
You can also use a path to a local repo, if it exists on every node.

```
connector.name=git
git.url=https://github.com/nineinchnick/trino-git.git
```

The catalog supports these properties:

| Property              | Description                                                                                     | Default                       |
|-----------------------|-------------------------------------------------------------------------------------------------|-------------------------------|
| `git.url`             | URL of the repository. Must not contain credentials. Previously called `metadata-uri`.         |                               |
| `git.username`        | Username used to authenticate to the repository.                                               |                               |
| `git.password`        | Password or access token used to authenticate to the repository.                               |                               |
| `git.cache-directory` | Directory where every node clones the repository. Only the user running Trino may access it.   | `${java.io.tmpdir}/trino-git` |

Keep credentials out of the catalog file by reading them from environment variables, for example
`git.password=${ENV:GIT_TOKEN}`. For GitHub, use a token as the password and any non-empty username.

After restarting Trino, the `git` catalog has the tables and views described below in the `default` schema.

To see who added and deleted the most lines:

```sql
SELECT
    c.author_name,
    count(DISTINCT c.object_id) AS commits,
    sum(s.added_lines) AS added_lines,
    sum(s.deleted_lines) AS deleted_lines
FROM commits c
JOIN diff_stats s ON s.commit_id = c.object_id
GROUP BY c.author_name
ORDER BY commits DESC
LIMIT 5;
```

See [examples/achievements.sql](examples/achievements.sql) for more complex queries.

# Tables

The `commits`, `diff_stats` and `trees` tables read every commit reachable from any ref:
`HEAD`, local branches, remote-tracking branches and tags.

## `branches`

Local and remote-tracking branches.

| Column      | Type      | Description                                         |
|-------------|-----------|-----------------------------------------------------|
| `object_id` | `varchar` | Id of the commit the branch points to               |
| `name`      | `varchar` | Full ref name, like `refs/heads/main`               |
| `is_merged` | `boolean` | Whether the branch is merged into the `HEAD` commit |

## `commits`

| Column            | Type                          | Description               |
|-------------------|-------------------------------|---------------------------|
| `object_id`       | `varchar`                     | Commit id                 |
| `author_name`     | `varchar`                     | Author name               |
| `author_email`    | `varchar`                     | Author email              |
| `committer_name`  | `varchar`                     | Committer name            |
| `committer_email` | `varchar`                     | Committer email           |
| `message`         | `varchar`                     | Full commit message       |
| `parents`         | `array(varchar)`              | Ids of the parent commits |
| `tree_id`         | `varchar`                     | Id of the root tree       |
| `commit_time`     | `timestamp(0) with time zone` | Commit time               |

## `diff_stats`

Files changed in every commit, with renames detected.

| Column             | Type      | Description                                                         |
|--------------------|-----------|---------------------------------------------------------------------|
| `commit_id`        | `varchar` | Commit id                                                           |
| `old_commit_id`    | `varchar` | Id of the commit it is compared with, see Known issues              |
| `object_id`        | `varchar` | Id of the new file contents                                         |
| `path_name`        | `varchar` | New file path                                                       |
| `old_path_name`    | `varchar` | Old file path                                                       |
| `change_type`      | `varchar` | One of `Add`, `Modify`, `Delete`, `Rename`, `Copy`                  |
| `similarity_score` | `integer` | Similarity of the old and new file for renames and copies, 0 to 100 |
| `added_lines`      | `integer` | Number of added lines                                               |
| `deleted_lines`    | `integer` | Number of deleted lines                                             |

## `objects`

Contents of every file (blob).

| Column      | Type        | Description   |
|-------------|-------------|---------------|
| `object_id` | `varchar`   | Blob id       |
| `contents`  | `varbinary` | File contents |

## `tags`

| Column      | Type                          | Description                                                  |
|-------------|-------------------------------|--------------------------------------------------------------|
| `object_id` | `varchar`                     | Id of the object the tag points to: a tag object or a commit |
| `name`      | `varchar`                     | Full ref name, like `refs/tags/v1.0`                         |
| `tag_time`  | `timestamp(0) with time zone` | Time of an annotated tag, `NULL` for lightweight tags        |

## `trees`

All files in every commit.

| Column        | Type      | Description                                                   |
|---------------|-----------|---------------------------------------------------------------|
| `commit_id`   | `varchar` | Commit id                                                     |
| `object_type` | `varchar` | One of `Normal File`, `Executable File`, `Symlink`, `Gitlink` |
| `object_id`   | `varchar` | Id of the file contents                                       |
| `file_name`   | `varchar` | File name                                                     |
| `path_name`   | `varchar` | Full file path                                                |
| `attributes`  | `varchar` | Git attributes of the file, from `.gitattributes`             |
| `depth`       | `integer` | Number of parent directories of the file                      |

# Views

The connector also provides two views:
* `commit_stats` - every column of `commits`, with the number of `added_lines`, `deleted_lines` and `changed_files`,
  the average `similarity_score`, and all `change_types` from `diff_stats`.
* `idents` - groups the authors and committers that share a name or an email.
  Every row has the main `email` and `name`, and the other `extra_emails` and `extra_names`.

# Limitations

* One catalog reads one repository. To query more repositories, create a catalog for each one. See [#273](https://github.com/nineinchnick/trino-git/issues/273).
* Every worker clones the repository into `git.cache-directory`, and keeps it there.
* When joining `trees` with `commits`, put `commits` on the right side of the join, like
  `trees t JOIN commits c ON t.commit_id = c.object_id`. The connector doesn't provide table statistics, so Trino doesn't
  reorder such joins, and only this order lets Trino read the trees of the matching commits instead of all of them.
* Every table scan fetches from the remote repository. See [#264](https://github.com/nineinchnick/trino-git/issues/264).

# Known issues

* Selecting `commits.parents` fails, so the `commit_stats` view fails too.
* The `idents` view does not parse. See [#261](https://github.com/nineinchnick/trino-git/issues/261).
* `diff_stats` compares every commit with the next commit it reads, not with its parent, so it has wrong results for repositories with more than one branch. See [#258](https://github.com/nineinchnick/trino-git/issues/258).
* Local branches are not updated after the first clone. See [#263](https://github.com/nineinchnick/trino-git/issues/263).
* `objects` only has objects from pack files. See [#265](https://github.com/nineinchnick/trino-git/issues/265).

# Build

Run all the tests:

```bash
./mvnw test
```

Build the plugin ZIP package, `target/trino-git-<version>.zip`:

```bash
./mvnw clean package -DskipTests
```

The ZIP has a `trino-git-<version>` directory with the plugin and all its dependencies.
Unzip it, and copy that directory as `plugin/git` on every node in your Trino cluster.

# Deploy

An example command to run the Trino server with a locally built plugin and the `git` catalog:

```bash
VERSION=$(./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout)
TRINO_VERSION=$(./mvnw help:evaluate -Dexpression=dep.trino.version -q -DforceStdout)
unzip -o -d target target/trino-git-$VERSION.zip
docker run \
  -d \
  --name trino \
  -e REPO_URL=https://github.com/nineinchnick/trino-git.git \
  -v "$PWD/target/trino-git-$VERSION:/usr/lib/trino/plugin/git" \
  -v "$PWD/catalog/git.properties:/etc/trino/catalog/git.properties" \
  -p 8080:8080 \
  trinodb/trino:$TRINO_VERSION
```

Connect to that server using:

```bash
docker exec -it trino trino --catalog git --schema default
```

# References

If you're looking to analyze the structure or contents of a Git repo, [gitbase](https://github.com/src-d/gitbase) could be more suitable for such task.
It could even work with Trino, since Trino has a [MySQL connector](https://trino.io/docs/current/connector/mysql.html).

If you also want to analyze Github issues, pull requests (with review comments) or workflow runs and jobs,
check out the Github connector in [trino-rest](https://github.com/nineinchnick/trino-rest).

This effort is inspired by [Acha](https://github.com/someteam/acha), to be able to calculate achievements based on contents of a Git repository using SQL.
