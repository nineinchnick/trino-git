Trino git Connector
===================

[![Build Status](https://github.com/nineinchnick/trino-git/workflows/CI/badge.svg)](https://github.com/nineinchnick/trino-git/actions?query=workflow%3ACI+event%3Apush+branch%3Amaster)

This is a [Trino](http://trino.io/) connector to access git repos. Please keep in mind that this is not production ready and it was created for tests.

# Usage

Copy jar files in the target directory to the plugins directory on every node in your Trino cluster.
Create a `git.properties` file in your Trino catalog directory and point to a remote repo. You can also use a path to a local repo if it's available on every worker node.

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
	c.committer_name,
	COUNT(c.object_id) as commits
FROM
	diff_stats s
JOIN commits c ON
	c.object_id = s.commit_id
GROUP BY
	c.object_id,
	c.committer_name
HAVING
	SUM(s.added_lines) = 0;
```

Should return:
```
committer_name|commits|
--------------|-------|
Jan Waś       |      1|
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
  -v $src/target/trino-git-0.3-SNAPSHOT:/usr/lib/trino/plugin/git \
  -v $src/catalog:/usr/lib/trino/default/etc/catalog \
  -p 8080:8080 \
  --name trino \
  -d \
  trinodb/trino:351
```

Connect to that server using:
```bash
docker run -it --rm --link trino trinodb/trino:351 trino --server trino:8080 --catalog git --schema default
```

# References

If you're looking to analize the structure or contents of a Git repo, [gitbase](https://github.com/src-d/gitbase) could be more suitable for such task. It could even work with Trino, since Trino has a [MySQL connector](https://trino.io/docs/current/connector/mysql.html).

This effort is inspired by [Acha](https://github.com/someteam/acha), to be able to calculate this in SQL.
