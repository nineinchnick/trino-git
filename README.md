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

| Property              | Description                                                                                  | Default                       |
|-----------------------|----------------------------------------------------------------------------------------------|-------------------------------|
| `git.url`             | URL of the repository. Must not contain credentials. Previously called `metadata-uri`.       |                               |
| `git.username`        | Username used to authenticate to the repository.                                             |                               |
| `git.password`        | Password or access token used to authenticate to the repository.                             |                               |
| `git.cache-directory` | Directory where every node clones the repository. Only the user running Trino may access it. | `${java.io.tmpdir}/trino-git` |

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
`HEAD`, branches and tags of the remote repository.

## `branches`

Branches of the remote repository.

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

Files changed in every commit compared with its first parent, with renames detected.
Merge commits are compared only with their first parent, like `git log -p --first-parent`,
and root commits with an empty tree, so all their files are added.
Copies are not detected, so `change_type` is never `copied`.

| Column             | Type      | Description                                                         |
|--------------------|-----------|---------------------------------------------------------------------|
| `commit_id`        | `varchar` | Commit id                                                           |
| `old_commit_id`    | `varchar` | Id of the first parent commit, `NULL` for a root commit             |
| `object_id`        | `varchar` | Id of the new file contents, `NULL` for a deleted file              |
| `old_object_id`    | `varchar` | Id of the old file contents, `NULL` for an added file               |
| `path_name`        | `varchar` | New file path, `NULL` for a deleted file                            |
| `old_path_name`    | `varchar` | Old file path, `NULL` for an added file                             |
| `change_type`      | `varchar` | One of `added`, `modified`, `deleted`, `renamed`, `copied`          |
| `similarity_score` | `integer` | Similarity of the old and new file for renames and copies, 0 to 100 |
| `added_lines`      | `integer` | Number of added lines, `NULL` for binary files                      |
| `deleted_lines`    | `integer` | Number of deleted lines, `NULL` for binary files                    |

## `objects`

Contents of every file (blob) reachable from any branch or tag.

| Column      | Type        | Description                                                              |
|-------------|-------------|--------------------------------------------------------------------------|
| `object_id` | `varchar`   | Blob id                                                                  |
| `contents`  | `varbinary` | File contents, `NULL` for blobs too large to load, over 50 MB by default |

## `tags`

| Column      | Type                          | Description                                                                |
|-------------|-------------------------------|----------------------------------------------------------------------------|
| `object_id` | `varchar`                     | Id of the object the tag points to: a tag object or a commit               |
| `commit_id` | `varchar`                     | Id of the tagged commit, `NULL` if the tag points to a tree or a blob      |
| `name`      | `varchar`                     | Full ref name, like `refs/tags/v1.0`                                       |
| `tag_time`  | `timestamp(0) with time zone` | Time of an annotated tag, `NULL` for lightweight tags and tags without one |

## `trees`

All files and directories in every commit. Directories come before their contents.

| Column       | Type      | Description                                                          |
|--------------|-----------|----------------------------------------------------------------------|
| `commit_id`  | `varchar` | Commit id                                                            |
| `file_mode`  | `varchar` | One of `file`, `executable`, `symlink`, `submodule`, `directory`     |
| `mode`       | `varchar` | Raw file mode in octal, like `100644`, the same as in `git ls-tree`  |
| `object_id`  | `varchar` | Id of the file contents, the directory tree, or the submodule commit |
| `file_name`  | `varchar` | File name                                                            |
| `path_name`  | `varchar` | Full file path                                                       |
| `attributes` | `varchar` | Git attributes of the file, from `.gitattributes`                    |
| `depth`      | `integer` | Number of parent directories of the file                             |

## `idents`

Authors and committers grouped into people: names used with the same email, and emails used with the same name,
belong to the same person, also indirectly. Names and emails are first mapped using the `.mailmap` file from `HEAD`,
like `git shortlog` does, and the mapped identity becomes the main one.
Otherwise, the main identity is the first name and email in alphabetical order.

| Column         | Type             | Description                          |
|----------------|------------------|--------------------------------------|
| `email`        | `varchar`        | Main email                           |
| `name`         | `varchar`        | Main name                            |
| `extra_emails` | `array(varchar)` | Other emails used by the same person |
| `extra_names`  | `array(varchar)` | Other names used by the same person  |

# Views

The connector also provides a view:
* `commit_stats` - every column of `commits`, with the number of `added_lines`, `deleted_lines` and `changed_files`,
  the average `similarity_score` of renames and copies, and all `change_types` from `diff_stats`.

# Limitations

* One catalog reads one repository. To query more repositories, create a catalog for each one. See [#273](https://github.com/nineinchnick/trino-git/issues/273).
* Every worker clones the repository into `git.cache-directory`, and keeps it there.
* When joining `trees` with `commits`, put `commits` on the right side of the join, like
  `trees t JOIN commits c ON t.commit_id = c.object_id`. The connector doesn't provide table statistics, so Trino doesn't
  reorder such joins, and only this order lets Trino read the trees of the matching commits instead of all of them.
* Every table scan fetches from the remote repository. See [#264](https://github.com/nineinchnick/trino-git/issues/264).

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
