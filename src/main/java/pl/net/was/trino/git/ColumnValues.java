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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.type.MapType;
import org.eclipse.jgit.lib.PersonIdent;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * Converts values read from repositories to native Trino values.
 */
final class ColumnValues
{
    private ColumnValues() {}

    static Slice toSlice(Optional<String> value)
    {
        return value.map(Slices::utf8Slice).orElse(null);
    }

    static long toTimestamp(PersonIdent ident)
    {
        return packDateTimeWithZone(ident.getWhenAsInstant().toEpochMilli(), ident.getZoneOffset().getTotalSeconds() / 60);
    }

    static Block toVarcharArray(List<String> values)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, values.size());
        values.forEach(value -> VARCHAR.writeString(builder, value));
        return builder.build();
    }

    static SqlMap toVarcharMap(MapType type, Map<String, String> values)
    {
        return buildMapValue(type, values.size(), (keyBuilder, valueBuilder) -> values.forEach((key, value) -> {
            VARCHAR.writeString(keyBuilder, key);
            VARCHAR.writeString(valueBuilder, value);
        }));
    }
}
