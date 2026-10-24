/*
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
package pl.net.was.trino.git;

import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.predicate.Domain.multipleValues;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

@Execution(ExecutionMode.SAME_THREAD)
public class TestGitMetadata
{
    private static final String COMMIT_A = "080dfdf0aac7d302dc31d57f62942bb6533944f7";
    private static final String COMMIT_B = "c3b14e59f88d0d6597b98ee93cf61e7556d540a4";
    private static final String COMMIT_C = "8f9ec8eb5f9a4a5c42fbd8d0a1a31c0e7b8c4f12";
    private static final GitColumnHandle TREES_COMMIT_ID = new GitColumnHandle("commit_id", VARCHAR, 0);
    private static final GitColumnHandle TREES_OBJECT_ID = new GitColumnHandle("object_id", VARCHAR, 1);

    private GitTableHandle commitsTableHandle;
    private GitMetadata metadata;

    @BeforeEach
    public void setUp()
            throws IOException, GitAPIException, URISyntaxException
    {
        commitsTableHandle = new GitTableHandle("default", "commits", TupleDomain.all());

        String url = "fake.example";
        TestGitClient.setupRepo(URI.create(url));

        metadata = new GitMetadata(new CatalogName("test"), new GitClient());
    }

    @Test
    public void testListSchemaNames()
    {
        assertThat(metadata.listSchemaNames(SESSION)).containsOnly("default");
    }

    @Test
    public void testGetTableHandle()
    {
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("example", "unknown"), Optional.empty(), Optional.empty())).isNull();
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "numbers"), Optional.empty(), Optional.empty())).isNull();
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown"), Optional.empty(), Optional.empty())).isNull();
    }

    @Test
    public void testGetColumnHandles()
    {
        // known table
        assertThat(metadata.getColumnHandles(SESSION, commitsTableHandle)).isEqualTo(Map.of(
                "object_id", new GitColumnHandle("object_id", createUnboundedVarcharType(), 0),
                "author_name", new GitColumnHandle("author_name", createUnboundedVarcharType(), 1),
                "author_email", new GitColumnHandle("author_email", createUnboundedVarcharType(), 2),
                "committer_name", new GitColumnHandle("committer_name", createUnboundedVarcharType(), 3),
                "committer_email", new GitColumnHandle("committer_email", createUnboundedVarcharType(), 4),
                "message", new GitColumnHandle("message", createUnboundedVarcharType(), 5),
                "parents", new GitColumnHandle("parents", new ArrayType(createUnboundedVarcharType()), 6),
                "tree_id", new GitColumnHandle("tree_id", createUnboundedVarcharType(), 7),
                "commit_time", new GitColumnHandle("commit_time", createTimestampWithTimeZoneType(0), 8),
                "author_time", new GitColumnHandle("author_time", createTimestampWithTimeZoneType(0), 9)));

        // unknown table
        assertThatThrownBy(() -> metadata.getColumnHandles(SESSION, new GitTableHandle("unknown", "unknown", TupleDomain.all())))
                .isInstanceOf(TableNotFoundException.class);
    }

    @Test
    public void getTableMetadata()
    {
        // known table
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, commitsTableHandle);
        assertThat(tableMetadata.getTable().getSchemaName()).isEqualTo("default");
        assertThat(tableMetadata.getColumns())
                .extracting(ColumnMetadata::getName, ColumnMetadata::getType)
                .containsExactly(
                        tuple("object_id", createUnboundedVarcharType()),
                        tuple("author_name", createUnboundedVarcharType()),
                        tuple("author_email", createUnboundedVarcharType()),
                        tuple("committer_name", createUnboundedVarcharType()),
                        tuple("committer_email", createUnboundedVarcharType()),
                        tuple("message", createUnboundedVarcharType()),
                        tuple("parents", new ArrayType(createUnboundedVarcharType())),
                        tuple("tree_id", createUnboundedVarcharType()),
                        tuple("commit_time", createTimestampWithTimeZoneType(0)),
                        tuple("author_time", createTimestampWithTimeZoneType(0)));
        assertThat(tableMetadata.getColumns())
                .allSatisfy(column -> assertThat(column.getComment()).isNotEmpty());

        // unknown tables should produce null
        assertThat(metadata.getTableMetadata(SESSION, new GitTableHandle("unknown", "unknown", TupleDomain.all()))).isNull();
        assertThat(metadata.getTableMetadata(SESSION, new GitTableHandle("example", "unknown", TupleDomain.all()))).isNull();
        assertThat(metadata.getTableMetadata(SESSION, new GitTableHandle("unknown", "numbers", TupleDomain.all()))).isNull();
    }

    @Test
    public void testListTables()
    {
        // all schemas
        assertThat(Set.copyOf(metadata.listTables(SESSION, Optional.empty()))).isEqualTo(Set.of(
                new SchemaTableName("default", "commits"),
                new SchemaTableName("default", "branches"),
                new SchemaTableName("default", "diff_stats"),
                new SchemaTableName("default", "idents"),
                new SchemaTableName("default", "objects"),
                new SchemaTableName("default", "tags"),
                new SchemaTableName("default", "trees")));

        // unknown schema
        assertThatThrownBy(() -> metadata.listTables(SESSION, Optional.of("unknown")))
                .isInstanceOf(SchemaNotFoundException.class);
    }

    @Test
    public void getColumnMetadata()
    {
        ColumnMetadata actualColumn = metadata.getColumnMetadata(
                SESSION,
                commitsTableHandle,
                new GitColumnHandle("message", createUnboundedVarcharType(), 5));
        assertThat(actualColumn).isEqualTo(ColumnMetadata.builder()
                .setName("message")
                .setType(createUnboundedVarcharType())
                .setComment(Optional.of("Full commit message"))
                .build());
    }

    @Test
    public void testDropTableTable()
    {
        assertThatThrownBy(() -> metadata.dropTable(SESSION, commitsTableHandle)).isInstanceOf(TrinoException.class);
    }

    @Test
    public void testApplyFilterIgnoresObjectIdOfTrees()
    {
        GitTableHandle trees = new GitTableHandle("default", "trees", TupleDomain.all());
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(TREES_OBJECT_ID, singleValue(VARCHAR, utf8Slice(COMMIT_A)))));
        assertThat(metadata.applyFilter(SESSION, trees, constraint)).isEmpty();
    }

    @Test
    public void testApplyFilterIntersectsCommitIds()
    {
        GitTableHandle trees = new GitTableHandle("default", "trees", TupleDomain.all());
        ConstraintApplicationResult<ConnectorTableHandle> first = metadata.applyFilter(SESSION, trees, commitIdsConstraint(TREES_COMMIT_ID, COMMIT_A, COMMIT_B)).orElseThrow();
        assertThat(first.getRemainingFilter()).isEqualTo(TupleDomain.all());
        ConstraintApplicationResult<ConnectorTableHandle> second = metadata.applyFilter(SESSION, first.getHandle(), commitIdsConstraint(TREES_COMMIT_ID, COMMIT_B, COMMIT_C)).orElseThrow();

        TupleDomain<GitColumnHandle> constraint = ((GitTableHandle) second.getHandle()).constraint();
        assertThat(GitMetadata.getCommitIds(constraint, "commit_id")).contains(List.of(COMMIT_B));
    }

    @Test
    public void testApplyFilterKeepsOtherColumnsUnenforced()
    {
        GitTableHandle trees = new GitTableHandle("default", "trees", TupleDomain.all());
        Domain objectId = singleValue(VARCHAR, utf8Slice(COMMIT_C));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                TREES_COMMIT_ID, singleValue(VARCHAR, utf8Slice(COMMIT_A)),
                TREES_OBJECT_ID, objectId)));

        ConstraintApplicationResult<ConnectorTableHandle> result = metadata.applyFilter(SESSION, trees, constraint).orElseThrow();
        assertThat(result.getRemainingFilter()).isEqualTo(TupleDomain.withColumnDomains(Map.of(TREES_OBJECT_ID, objectId)));
        assertThat(GitMetadata.getCommitIds(((GitTableHandle) result.getHandle()).constraint(), "commit_id")).contains(List.of(COMMIT_A));
    }

    @Test
    public void testGetCommitIdsSkipsInvalidIds()
    {
        TupleDomain<GitColumnHandle> constraint = TupleDomain.withColumnDomains(Map.of(
                TREES_COMMIT_ID, multipleValues(VARCHAR, List.of(utf8Slice(COMMIT_A), utf8Slice(COMMIT_B.toUpperCase(ENGLISH)), utf8Slice(COMMIT_C.substring(0, 7))))));
        assertThat(GitMetadata.getCommitIds(constraint, "commit_id")).contains(List.of(COMMIT_A));
        assertThat(GitMetadata.getCommitIds(TupleDomain.none(), "commit_id")).contains(List.of());
        assertThat(GitMetadata.getCommitIds(TupleDomain.all(), "commit_id")).isEmpty();
    }

    private static Constraint commitIdsConstraint(GitColumnHandle column, String... commitIds)
    {
        List<Object> values = Stream.of(commitIds).map(Slices::utf8Slice).collect(toImmutableList());
        return new Constraint(TupleDomain.withColumnDomains(Map.of(column, multipleValues(VARCHAR, values))));
    }
}
