package sqlancer.synapsesql.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.synapsesql.SynapseSqlProvider.SynapseSqlGlobalState;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlColumn;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlTable;
import sqlancer.synapsesql.SynapseSqlToStringVisitor;
import sqlancer.synapsesql.ast.SynapseSqlExpression;

public final class SynapseSqlIndexGenerator {

    private SynapseSqlIndexGenerator() {
    }

    public static SQLQueryAdapter getQuery(SynapseSqlGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (Randomly.getBoolean()) {
            errors.add("Cant create unique index, table contains duplicate data on indexed column(s)");
            sb.append("UNIQUE ");
        }
        sb.append("INDEX ");
        sb.append(Randomly.fromOptions("i0", "i1", "i2", "i3", "i4")); // cannot query this information
        sb.append(" ON ");
        SynapseSqlTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        sb.append("(");
        List<SynapseSqlColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append(" ");
            if (Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(Randomly.fromOptions("ASC", "DESC"));
            }
        }
        sb.append(")");
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            Node<SynapseSqlExpression> expr = new SynapseSqlExpressionGenerator(globalState).setColumns(table.getColumns())
                    .generateExpression();
            sb.append(SynapseSqlToStringVisitor.asString(expr));
        }
        errors.add("already exists!");
        if (globalState.getDbmsSpecificOptions().testRowid) {
            errors.add("Cannot create an index on the rowid!");
        }
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
