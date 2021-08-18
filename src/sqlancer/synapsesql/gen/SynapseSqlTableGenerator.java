package sqlancer.synapsesql.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.synapsesql.SynapseSqlErrors;
import sqlancer.synapsesql.SynapseSqlProvider.SynapseSqlGlobalState;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlColumn;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlCompositeDataType;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlDataType;
import sqlancer.synapsesql.SynapseSqlToStringVisitor;
import sqlancer.synapsesql.ast.SynapseSqlExpression;

public class SynapseSqlTableGenerator {

    public SQLQueryAdapter getQuery(SynapseSqlGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        String tableName = globalState.getSchema().getFreeTableName();
        sb.append("CREATE TABLE ");
        sb.append(tableName);
        sb.append("(");
        List<SynapseSqlColumn> columns = getNewColumns();
        UntypedExpressionGenerator<Node<SynapseSqlExpression>, SynapseSqlColumn> gen = new SynapseSqlExpressionGenerator(
                globalState).setColumns(columns);
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append(" ");
            sb.append(columns.get(i).getType());
            if (globalState.getDbmsSpecificOptions().testCollate && Randomly.getBooleanWithRatherLowProbability()
                    && columns.get(i).getType().getPrimitiveDataType() == SynapseSqlDataType.VARCHAR) {
                sb.append(" COLLATE ");
                sb.append(getRandomCollate());
            }
            if (globalState.getDbmsSpecificOptions().testIndexes && Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" UNIQUE");
            }
            if (globalState.getDbmsSpecificOptions().testNotNullConstraints
                    && Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" NOT NULL");
            }
            if (globalState.getDbmsSpecificOptions().testCheckConstraints
                    && Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" CHECK(");
                sb.append(SynapseSqlToStringVisitor.asString(gen.generateExpression()));
                SynapseSqlErrors.addExpressionErrors(errors);
                sb.append(")");
            }
            if (Randomly.getBoolean() && globalState.getDbmsSpecificOptions().testDefaultValues) {
                sb.append(" DEFAULT(");
                sb.append(SynapseSqlToStringVisitor.asString(gen.generateConstant()));
                SynapseSqlErrors.addExpressionErrors(errors);
                sb.append(")");
            }
        }
        if (globalState.getDbmsSpecificOptions().testIndexes && Randomly.getBoolean()) {
            errors.add("Invalid type for index");
            List<SynapseSqlColumn> primaryKeyColumns = Randomly.nonEmptySubset(columns);
            sb.append(", PRIMARY KEY(");
            sb.append(primaryKeyColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
            sb.append(")");
        }
        sb.append(")");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    public static String getRandomCollate() {
        return Randomly.fromOptions("NOCASE", "NOACCENT", "NOACCENT.NOCASE", "C", "POSIX");
    }

    private static List<SynapseSqlColumn> getNewColumns() {
        List<SynapseSqlColumn> columns = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            SynapseSqlCompositeDataType columnType = SynapseSqlCompositeDataType.getRandom();
            columns.add(new SynapseSqlColumn(columnName, columnType, false, false));
        }
        return columns;
    }

}
