Trino git Connector
===================

[![Build Status](https://github.com/nineinchnick/trino-git/workflows/CI/badge.svg)](https://github.com/nineinchnick/trino-git/actions?query=workflow%3ACI+event%3Apush+branch%3Amaster)
This is a [Trino](http://trino.io/) connector to access git repos. Please keep in mind that this is not production ready and it was created for tests.

# Query
You need to specify file type by schema name and use absolute path.
```sql
select
  *
from
 git.commits."file:///tmp/repo"
;

select
  *
from
 git.commits."https://github.com/trinodb/trino.git"
;
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
  -v $src/target/trino-git-0.1-SNAPSHOT:/usr/lib/presto/plugin/git \
  -v $src/catalog:/usr/lib/presto/default/etc/catalog \
  -p 8080:8080 \
  --name presto \
  -d \
  prestosql/presto
```

Connect to that server using:
```bash
docker run -it --rm --link presto prestosql/presto presto --server presto:8080 --catalog git --schema default
```
