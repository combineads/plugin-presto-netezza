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
package com.facebook.presto.plugin.netezza;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.spi.type.Type;
import org.netezza.Driver;

import javax.inject.Inject;

import java.sql.SQLException;

public class NetezzaClient
        extends
        BaseJdbcClient
{
    @Inject
    public NetezzaClient(JdbcConnectorId connectorId, BaseJdbcConfig config) throws SQLException
    {
        super(connectorId, config, "\"", new Driver());
    }

    @Override
    protected String toSqlType(Type type)
    {
        String sqlType = super.toSqlType(type);
        switch (sqlType) {
            case "tinyint":
                return "byteint";
            case "time with timezone":
                return "time with time zone";
            case "varbinary":
                return "varchar";
            case "timestamp with timezone":
                return "timestamp";
        }
        return sqlType;
    }
}
