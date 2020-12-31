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
package pl.net.was.presto.git;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestGitMetadata
{
    private GitTableHandle commitsTableHandle;
    private GitMetadata metadata;

    @BeforeMethod
    public void setUp()
            throws IOException, GitAPIException, URISyntaxException
    {
        commitsTableHandle = new GitTableHandle("default", "commits");

        String url = "fake.example";
        File localPath;
        try {
            localPath = GitRecordSet.ensureDir(url);
        }
        catch (IOException ignored) {
            return;
        }
        if (localPath.exists()) {
            Files.walk(localPath.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
        Git.init().setDirectory(localPath).call();

        GitConfig config = new GitConfig();
        config.setMetadata(new URI(url));
        GitClient client = new GitClient(config);
        metadata = new GitMetadata(client);
    }

    @Test
    public void testListSchemaNames()
    {
        assertThat(metadata.listSchemaNames(SESSION)).containsOnly("default");
    }

    @Test
    public void testGetTableHandle()
    {
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("example", "unknown")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "numbers")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown")));
    }

    @Test
    public void testGetColumnHandles()
    {
        // known table
        assertEquals(metadata.getColumnHandles(SESSION, commitsTableHandle), Map.of(
                "one", new GitColumnHandle("one", createUnboundedVarcharType(), 0),
                "1", new GitColumnHandle("1", createUnboundedVarcharType(), 1)));

        // unknown table
        try {
            metadata.getColumnHandles(SESSION, new GitTableHandle("unknown", "unknown"));
            fail("Expected getColumnHandle of unknown table to throw a TableNotFoundException");
        }
        catch (SchemaNotFoundException expected) {
        }
        try {
            metadata.getColumnHandles(SESSION, new GitTableHandle("csv", "unknown"));
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
        assertEquals(tableMetadata.getTable().getSchemaName(), "default");
        assertEquals(tableMetadata.getColumns(), List.of(
                new ColumnMetadata("object_id", createUnboundedVarcharType()),
                new ColumnMetadata("author", createUnboundedVarcharType()),
                new ColumnMetadata("committer", createUnboundedVarcharType()),
                new ColumnMetadata("message", createUnboundedVarcharType()),
                new ColumnMetadata("commit_time", createTimestampWithTimeZoneType(3))));

        // unknown tables should produce null
        assertNull(metadata.getTableMetadata(SESSION, new GitTableHandle("unknown", "unknown")));
        assertNull(metadata.getTableMetadata(SESSION, new GitTableHandle("example", "unknown")));
        assertNull(metadata.getTableMetadata(SESSION, new GitTableHandle("unknown", "numbers")));
    }

    @Test
    public void testListTables()
    {
        // all schemas
        assertEquals(Set.copyOf(metadata.listTables(SESSION, Optional.empty())), Set.of("default.commits", "default.branches"));

        // unknown schema
        assertEquals(Set.copyOf(metadata.listTables(SESSION, Optional.of("unknown"))), Set.of("default.commits", "default.branches"));
    }

    @Test
    public void getColumnMetadata()
    {
        assertEquals(metadata.getColumnMetadata(SESSION, commitsTableHandle, new GitColumnHandle("text", createUnboundedVarcharType(), 0)),
                new ColumnMetadata("text", createUnboundedVarcharType()));

        // example connector assumes that the table handle and column handle are
        // properly formed, so it will return a metadata object for any
        // GitTableHandle and GitColumnHandle passed in.  This is on because
        // it is not possible for the Presto Metadata system to create the handles
        // directly.
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testCreateTable()
    {
        metadata.createTable(
                SESSION,
                new ConnectorTableMetadata(
                        new SchemaTableName("example", "foo"),
                        List.of(new ColumnMetadata("text", createUnboundedVarcharType()))),
                false);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testDropTableTable()
    {
        metadata.dropTable(SESSION, commitsTableHandle);
    }
}
