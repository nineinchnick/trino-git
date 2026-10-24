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

import io.trino.spi.type.Type;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * A column of a table, with a function reading its value from a row, as the native Trino value of the column type.
 */
public record TableColumn<T>(GitColumn column, Function<T, Object> getter)
{
    public TableColumn
    {
        requireNonNull(column, "column is null");
        requireNonNull(getter, "getter is null");
    }

    public static <T> TableColumn<T> column(String name, Type type, String comment, Function<T, Object> getter)
    {
        return new TableColumn<>(new GitColumn(name, type, comment), getter);
    }
}
