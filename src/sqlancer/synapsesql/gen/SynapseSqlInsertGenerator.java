package sqlancer.synapsesql.gen;

import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.synapsesql.SynapseSqlErrors;
import sqlancer.synapsesql.SynapseSqlProvider.SynapseSqlGlobalState;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlColumn;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlTable;
import sqlancer.synapsesql.SynapseSqlToStringVisitor;

public class SynapseSqlInsertGenerator extends AbstractInsertGenerator<SynapseSqlColumn> {

    private final SynapseSqlGlobalState globalState;
    private final ExpectedErrors errors = new ExpectedErrors();
    private final SynapseSqlExpressionGenerator expressionGenerator;

    public SynapseSqlInsertGenerator(SynapseSqlGlobalState globalState) {
        this.globalState = globalState;
        this.expressionGenerator = new SynapseSqlExpressionGenerator(globalState);
    }

    public static SQLQueryAdapter getQuery(SynapseSqlGlobalState globalState) {
        return new SynapseSqlInsertGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        sb.append("INSERT INTO ");
        SynapseSqlTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        //List<SynapseSqlColumn> columns = table.getRandomNonEmptyColumnSubset();
        List<SynapseSqlColumn> columns = table.getColumns();
        sb.append(table.getName());
        sb.append("(");
        sb.append(columns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
        sb.append(")");
        sb.append(" VALUES ");
        insertColumns(columns);
        SynapseSqlErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void insertValue(SynapseSqlColumn tiDBColumn) {
        sb.append(SynapseSqlToStringVisitor.asString(expressionGenerator.generateConstant(tiDBColumn.getType())));
    }

}
