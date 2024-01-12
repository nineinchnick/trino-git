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

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
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
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

@Execution(ExecutionMode.SAME_THREAD)
public class TestGitMetadata
{
    private GitTableHandle commitsTableHandle;
    private GitMetadata metadata;

    @BeforeEach
    public void setUp()
            throws IOException, GitAPIException, URISyntaxException
    {
        commitsTableHandle = new GitTableHandle("default", "commits", Optional.empty(), OptionalLong.empty());

        String url = "fake.example";
        TestGitClient.setupRepo(URI.create(url));

        GitConfig config = new GitConfig();
        config.setUri(new URI(url));
        GitClient client = new GitClient(config);
        metadata = new GitMetadata("test", client);
    }

    @Test
    public void testListSchemaNames()
    {
        assertThat(metadata.listSchemaNames(SESSION)).containsOnly("default");
    }

    @Test
    public void testGetTableHandle()
    {
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("example", "unknown"))).isNull();
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "numbers"))).isNull();
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown"))).isNull();
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
                "commit_time", new GitColumnHandle("commit_time", createTimestampWithTimeZoneType(0), 8)));

        // unknown table
        try {
            metadata.getColumnHandles(SESSION, new GitTableHandle("unknown", "unknown", Optional.empty(), OptionalLong.empty()));
            fail("Expected getColumnHandle of unknown table to throw a TableNotFoundException");
        }
        catch (TableNotFoundException expected) {
        }
    }

    @Test
    public void getTableMetadata()
    {
        // known table
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, commitsTableHandle);
        assertThat(tableMetadata.getTable().getSchemaName()).isEqualTo("default");
        assertThat(tableMetadata.getColumns()).isEqualTo(List.of(
                new ColumnMetadata("object_id", createUnboundedVarcharType()),
                new ColumnMetadata("author_name", createUnboundedVarcharType()),
                new ColumnMetadata("author_email", createUnboundedVarcharType()),
                new ColumnMetadata("committer_name", createUnboundedVarcharType()),
                new ColumnMetadata("committer_email", createUnboundedVarcharType()),
                new ColumnMetadata("message", createUnboundedVarcharType()),
                new ColumnMetadata("parents", new ArrayType(createUnboundedVarcharType())),
                new ColumnMetadata("tree_id", createUnboundedVarcharType()),
                new ColumnMetadata("commit_time", createTimestampWithTimeZoneType(0))));

        // unknown tables should produce null
        assertThat(metadata.getTableMetadata(SESSION, new GitTableHandle("unknown", "unknown", Optional.empty(), OptionalLong.empty()))).isNull();
        assertThat(metadata.getTableMetadata(SESSION, new GitTableHandle("example", "unknown", Optional.empty(), OptionalLong.empty()))).isNull();
        assertThat(metadata.getTableMetadata(SESSION, new GitTableHandle("unknown", "numbers", Optional.empty(), OptionalLong.empty()))).isNull();
    }

    @Test
    public void testListTables()
    {
        // all schemas
        assertThat(Set.copyOf(metadata.listTables(SESSION, Optional.empty()))).isEqualTo(Set.of(
                new SchemaTableName("default", "commits"),
                new SchemaTableName("default", "branches"),
                new SchemaTableName("default", "diff_stats"),
                new SchemaTableName("default", "objects"),
                new SchemaTableName("default", "tags"),
                new SchemaTableName("default", "trees")));

        // unknown schema
        try {
            metadata.listTables(SESSION, Optional.of("unknown"));
            fail("Expected listTables of unknown schema to throw a SchemaNotFoundException");
        }
        catch (SchemaNotFoundException expected) {
        }
    }

    @Test
    public void getColumnMetadata()
    {
        ColumnMetadata actualColumn = metadata.getColumnMetadata(SESSION, commitsTableHandle,
                new GitColumnHandle("text", createUnboundedVarcharType(), 0));
        assertThat(actualColumn).isEqualTo(new ColumnMetadata("text", createUnboundedVarcharType()));

        // example connector assumes that the table handle and column handle are
        // properly formed, so it will return a metadata object for any
        // GitTableHandle and GitColumnHandle passed in.  This is on because
        // it is not possible for the Trino Metadata system to create the handles
        // directly.
    }

    @Test
    public void testCreateTable()
    {
        assertThatThrownBy(() -> metadata.createTable(
                SESSION,
                new ConnectorTableMetadata(
                        new SchemaTableName("example", "foo"),
                        List.of(new ColumnMetadata("text", createUnboundedVarcharType()))),
                false)).isInstanceOf(TrinoException.class);
    }

    @Test
    public void testDropTableTable()
    {
        assertThatThrownBy(() -> metadata.dropTable(SESSION, commitsTableHandle)).isInstanceOf(TrinoException.class);
    }
}
