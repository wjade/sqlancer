package sqlancer.synapsesql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.synapsesql.SynapseSqlProvider.SynapseSqlGlobalState;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlTable;

public class SynapseSqlSchema extends AbstractSchema<SynapseSqlGlobalState, SynapseSqlTable> {

    public enum SynapseSqlDataType {

        INT, VARCHAR, BOOLEAN, FLOAT, DATE, TIMESTAMP;

        public static SynapseSqlDataType getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public static class SynapseSqlCompositeDataType {

        private final SynapseSqlDataType dataType;

        private final int size;

        public SynapseSqlCompositeDataType(SynapseSqlDataType dataType, int size) {
            this.dataType = dataType;
            this.size = size;
        }

        public SynapseSqlDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getSize() {
            if (size == -1) {
                throw new AssertionError(this);
            }
            return size;
        }

        public static SynapseSqlCompositeDataType getRandom() {
            SynapseSqlDataType type = SynapseSqlDataType.getRandom();
            int size = -1;
            switch (type) {
            case INT:
                size = Randomly.fromOptions(1, 2, 4, 8);
                break;
            case FLOAT:
                size = Randomly.fromOptions(4, 8);
                break;
            case BOOLEAN:
            case VARCHAR:
            case DATE:
            case TIMESTAMP:
                size = 0;
                break;
            default:
                throw new AssertionError(type);
            }

            return new SynapseSqlCompositeDataType(type, size);
        }

        @Override
        public String toString() {
            switch (getPrimitiveDataType()) {
            case INT:
                switch (size) {
                case 8:
                    return Randomly.fromOptions("BIGINT", "INT8");
                case 4:
                    return Randomly.fromOptions("INTEGER", "INT", "INT4", "SIGNED");
                case 2:
                    return Randomly.fromOptions("SMALLINT", "INT2");
                case 1:
                    return Randomly.fromOptions("TINYINT", "INT1");
                default:
                    throw new AssertionError(size);
                }
            case VARCHAR:
                return "VARCHAR";
            case FLOAT:
                switch (size) {
                case 8:
                    return Randomly.fromOptions("DOUBLE");
                case 4:
                    return Randomly.fromOptions("REAL", "FLOAT4");
                default:
                    throw new AssertionError(size);
                }
            case BOOLEAN:
                return Randomly.fromOptions("BOOLEAN", "BOOL");
            case TIMESTAMP:
                return Randomly.fromOptions("TIMESTAMP", "DATETIME");
            case DATE:
                return Randomly.fromOptions("DATE");
            default:
                throw new AssertionError(getPrimitiveDataType());
            }
        }

    }

    public static class SynapseSqlColumn extends AbstractTableColumn<SynapseSqlTable, SynapseSqlCompositeDataType> {

        private final boolean isPrimaryKey;
        private final boolean isNullable;

        public SynapseSqlColumn(String name, SynapseSqlCompositeDataType columnType, boolean isPrimaryKey, boolean isNullable) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.isNullable = isNullable;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public boolean isNullable() {
            return isNullable;
        }

    }

    public static class SynapseSqlTables extends AbstractTables<SynapseSqlTable, SynapseSqlColumn> {

        public SynapseSqlTables(List<SynapseSqlTable> tables) {
            super(tables);
        }

    }

    public SynapseSqlSchema(List<SynapseSqlTable> databaseTables) {
        super(databaseTables);
    }

    public SynapseSqlTables getRandomTableNonEmptyTables() {
        return new SynapseSqlTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    private static SynapseSqlCompositeDataType getColumnType(String typeString) {
        SynapseSqlDataType primitiveType;
        int size = -1;
        if (typeString.startsWith("DECIMAL")) { // Ugly hack
            return new SynapseSqlCompositeDataType(SynapseSqlDataType.FLOAT, 8);
        }
        switch (typeString) {
        case "INTEGER":
            primitiveType = SynapseSqlDataType.INT;
            size = 4;
            break;
        case "SMALLINT":
            primitiveType = SynapseSqlDataType.INT;
            size = 2;
            break;
        case "BIGINT":
        case "HUGEINT": // TODO: 16-bit int
            primitiveType = SynapseSqlDataType.INT;
            size = 8;
            break;
        case "TINYINT":
            primitiveType = SynapseSqlDataType.INT;
            size = 1;
            break;
        case "VARCHAR":
            primitiveType = SynapseSqlDataType.VARCHAR;
            break;
        case "FLOAT":
            primitiveType = SynapseSqlDataType.FLOAT;
            size = 4;
            break;
        case "DOUBLE":
            primitiveType = SynapseSqlDataType.FLOAT;
            size = 8;
            break;
        case "BOOLEAN":
            primitiveType = SynapseSqlDataType.BOOLEAN;
            break;
        case "DATE":
            primitiveType = SynapseSqlDataType.DATE;
            break;
        case "TIMESTAMP":
            primitiveType = SynapseSqlDataType.TIMESTAMP;
            break;
        case "INTERVAL":
            throw new IgnoreMeException();
        // TODO: caused when a view contains a computation like ((TIMESTAMP '1970-01-05 11:26:57')-(TIMESTAMP
        // '1969-12-29 06:50:27'))
        default:
            throw new AssertionError(typeString);
        }
        return new SynapseSqlCompositeDataType(primitiveType, size);
    }

    public static class SynapseSqlTable extends AbstractRelationalTable<SynapseSqlColumn, TableIndex, SynapseSqlGlobalState> {

        public SynapseSqlTable(String tableName, List<SynapseSqlColumn> columns, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

    }

    public static SynapseSqlSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<SynapseSqlTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            if (DBMSCommon.matchesIndexName(tableName)) {
                continue; // TODO: unexpected?
            }
            List<SynapseSqlColumn> databaseColumns = getTableColumns(con, tableName);
            boolean isView = tableName.startsWith("v");
            SynapseSqlTable t = new SynapseSqlTable(tableName, databaseColumns, isView);
            for (SynapseSqlColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);

        }
        return new SynapseSqlSchema(databaseTables);
    }

    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT * FROM sqlite_master()")) {
                while (rs.next()) {
                    tableNames.add(rs.getString("name"));
                }
            }
        }
        return tableNames;
    }

    private static List<SynapseSqlColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<SynapseSqlColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format("SELECT * FROM pragma_table_info('%s');", tableName))) {
                while (rs.next()) {
                    String columnName = rs.getString("name");
                    String dataType = rs.getString("type");
                    boolean isNullable = rs.getString("notnull").contentEquals("false");
                    boolean isPrimaryKey = rs.getString("pk").contains("true");
                    SynapseSqlColumn c = new SynapseSqlColumn(columnName, getColumnType(dataType), isPrimaryKey, isNullable);
                    columns.add(c);
                }
            }
        }
        if (columns.stream().noneMatch(c -> c.isPrimaryKey())) {
            // https://github.com/cwida/duckdb/issues/589
            // https://github.com/cwida/duckdb/issues/588
            // TODO: implement an option to enable/disable rowids
            columns.add(new SynapseSqlColumn("rowid", new SynapseSqlCompositeDataType(SynapseSqlDataType.INT, 4), false, false));
        }
        return columns;
    }

}
