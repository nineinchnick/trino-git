Trino git Connector
===================

[![Build Status](https://github.com/nineinchnick/trino-git/actions/workflows/release.yaml/badge.svg)](https://github.com/nineinchnick/trino-git/actions/workflows/release.yaml)

This is a [Trino](http://trino.io/) connector to access git repos. Please keep in mind that this is not production ready and it was created for tests.

# Quick Start

To run a Docker container with the connector, run the following:
```bash
docker run \
  -d \
  --name trino-git \
  -e REPO_URL=https://github.com/nineinchnick/trino-rest.git \
  -p 8080:8080 \
  nineinchnick/trino-git:0.22
```

Then use your favourite SQL client to connect to Trino running at http://localhost:8080

# Usage

Download one of the ZIP packages, unzip it and copy the `trino-git-0.22` directory to the plugin directory on every node in your Trino cluster.
Create a `github.properties` file in your Trino catalog directory and point to a remote repo.
You can also use a path to a local repo if it's available on every worker node.

```
connector.name=git
metadata-uri=https://github.com/nineinchnick/trino-git.git
```

After reloading Trino, you should be able to connect to the `git` catalog and see the following tables in the `default` schema:
* `branches`
* `commits` - all commits from every branch, with author, committer, message and commit time
* `diff_stats` - any files modified, deleted or renamed in every commit, with number of added and/or deleted lines
* `objects` - every file contents
* `tags`
* `trees` - all files in every commit, with file mode and attributes

To see who has commits with only deleted lines:

```sql
SELECT
    i.name,
    i.email,
    min(c.commit_time) FILTER (WHERE c.added_lines = 0 AND c.deleted_lines != 0) AS first_delete_only_commit_at,
    count(*) FILTER (WHERE c.added_lines = 0 AND c.deleted_lines != 0) AS delete_only_commit_count,
    CAST(count(*) FILTER (WHERE c.added_lines = 0 AND c.deleted_lines != 0) AS double) / CAST(COUNT(*) AS double) AS delete_only_commit_ratio
FROM
    commit_stats c
JOIN idents i ON
    c.author_email = i.email OR CONTAINS(i.extra_emails, c.author_email)
GROUP BY
    i.name,
    i.email
HAVING
    count(*) FILTER (WHERE c.added_lines = 0 AND c.deleted_lines != 0) != 0
ORDER BY
    i.name,
    i.email;
```

Should return:
```
name   |email         |first_delete_only_commit_at|delete_only_commit_count|delete_only_commit_ratio|
-------|--------------|---------------------------|------------------------|------------------------|
Jan Was|jan@was.net.pl|        2021-01-09 23:22:28|                       2|     0.08695652173913043|
```

# Build

Run all the unit test classes.
```
mvn test
```

Creates a deployable jar file
```
mvn clean compile package
```

Copy jar files in target directory to use git connector in your Trino cluster.
```
cp -p target/*.jar ${PLUGIN_DIRECTORY}/git/
```

# Deploy

An example command to run the Trino server with the git plugin and catalog enabled:

```bash
src=$(git rev-parse --show-toplevel)
docker run \
  -v $src/target/trino-git-0.20-SNAPSHOT:/usr/lib/trino/plugin/git \
  -v $src/catalog:/usr/lib/trino/default/etc/catalog \
  -p 8080:8080 \
  --name trino \
  -d \
  trinodb/trino:389
```

Connect to that server using:
```bash
docker run -it --rm --link trino trinodb/trino:389 trino --server trino:8080 --catalog git --schema default
```

# References

If you're looking to analize the structure or contents of a Git repo, [gitbase](https://github.com/src-d/gitbase) could be more suitable for such task.
It could even work with Trino, since Trino has a [MySQL connector](https://trino.io/docs/current/connector/mysql.html).

If you also want to analyze Github issues, pull requests (with review comments) or workflow runs and jobs,
check out the Github connector in [trino-rest](https://github.com/nineinchnick/trino-rest).

This effort is inspired by [Acha](https://github.com/someteam/acha), to be able to calculate achievements based on contents of a Git repository using SQL.
