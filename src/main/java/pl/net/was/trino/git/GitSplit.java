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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class GitSplit
        implements ConnectorSplit
{
    // split needs to track the URI from config to use it in RecordSet
    private final URI uri;
    // split needs to track for which table it was created for to use it in RecordSetProvider
    private final String tableName;
    private final Optional<List<String>> commitIds;

    @JsonCreator
    public GitSplit(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("uri") URI uri,
            @JsonProperty("commitIds") Optional<List<String>> commitIds)
    {
        this.tableName = requireNonNull(tableName, "table name is null");
        this.uri = requireNonNull(uri, "uri is null");
        this.commitIds = requireNonNull(commitIds, "commitIds is null");
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public URI getUri()
    {
        return uri;
    }

    @JsonProperty
    public Optional<List<String>> getCommitIds()
    {
        return commitIds;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        // only http or https is remotely accessible
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return List.of();
    }
}
