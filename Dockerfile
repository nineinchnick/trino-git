ARG TRINO_VERSION
FROM trinodb/trino:$TRINO_VERSION

ARG VERSION

RUN rm -rf /usr/lib/trino/plugin/{accumulo,atop,bigquery,blackhole,cassandra,clickhouse,delta-lake,druid,elasticsearch,example-http,geospatial,google-sheets,hive,http-event-listener,iceberg,kafka,kinesis,kudu} \
    && rm -rf /usr/lib/trino/plugin/{local-file,memsql,ml,mongodb,mysql,oracle,password-authenticatorsphoenix5,pinot,postgresql,prometheus,raptor-legacy,redis,redshift,resource-group-managers,session-property-managers} \
    && rm -rf /usr/lib/trino/plugin/{sqlserver,teradata-functions,thrift,tpcds,tpch} \
    && rm -rf /etc/trino/catalog/{tpcds,tpch}.properties \
    && ls -la /usr/lib/trino/plugin

ADD target/trino-git-$VERSION/ /usr/lib/trino/plugin/git/
ADD catalog/git.properties /etc/trino/catalog/git.properties
