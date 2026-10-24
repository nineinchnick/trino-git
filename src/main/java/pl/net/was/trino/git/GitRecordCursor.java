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
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Returns rows read by {@link GitReader}, with the values of the selected columns.
 */
public class GitRecordCursor<T>
        implements RecordCursor
{
    private final RowIterator<T> rows;
    private final List<Type> types;
    private final List<Function<T, Object>> getters;
    private T row;

    public GitRecordCursor(RowIterator<T> rows, List<Type> types, List<Function<T, Object>> getters)
    {
        this.rows = requireNonNull(rows, "rows is null");
        this.types = List.copyOf(types);
        this.getters = List.copyOf(getters);
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        return types.get(field);
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!rows.hasNext()) {
            return false;
        }
        row = rows.next();
        return true;
    }

    @Override
    public boolean getBoolean(int field)
    {
        return (boolean) getValue(field);
    }

    @Override
    public long getLong(int field)
    {
        return (long) getValue(field);
    }

    @Override
    public double getDouble(int field)
    {
        return (double) getValue(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        return (Slice) getValue(field);
    }

    @Override
    public Object getObject(int field)
    {
        return getValue(field);
    }

    @Override
    public boolean isNull(int field)
    {
        return getValue(field) == null;
    }

    @Override
    public void close()
    {
        rows.close();
    }

    private Object getValue(int field)
    {
        checkState(row != null, "Cursor has not been advanced yet");
        return getters.get(field).apply(row);
    }
}
