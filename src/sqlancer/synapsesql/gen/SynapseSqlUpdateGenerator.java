package sqlancer.synapsesql.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.synapsesql.SynapseSqlErrors;
import sqlancer.synapsesql.SynapseSqlProvider.SynapseSqlGlobalState;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlColumn;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlTable;
import sqlancer.synapsesql.SynapseSqlToStringVisitor;
import sqlancer.synapsesql.ast.SynapseSqlExpression;

public final class SynapseSqlUpdateGenerator {

    private SynapseSqlUpdateGenerator() {
    }

    public static SQLQueryAdapter getQuery(SynapseSqlGlobalState globalState) {
        StringBuilder sb = new StringBuilder("UPDATE ");
        ExpectedErrors errors = new ExpectedErrors();
        SynapseSqlTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        SynapseSqlExpressionGenerator gen = new SynapseSqlExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append(" SET ");
        List<SynapseSqlColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append("=");
            Node<SynapseSqlExpression> expr;
            if (Randomly.getBooleanWithSmallProbability()) {
                expr = gen.generateExpression();
                SynapseSqlErrors.addExpressionErrors(errors);
            } else {
                expr = gen.generateConstant();
            }
            sb.append(SynapseSqlToStringVisitor.asString(expr));
        }
        SynapseSqlErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
