package sqlancer.synapsesql.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.synapsesql.SynapseSqlErrors;
import sqlancer.synapsesql.SynapseSqlProvider.SynapseSqlGlobalState;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlColumn;
import sqlancer.synapsesql.SynapseSqlToStringVisitor;
import sqlancer.synapsesql.ast.SynapseSqlExpression;

public class SynapseSqlQueryPartitioningGroupByTester extends SynapseSqlQueryPartitioningBase {

    public SynapseSqlQueryPartitioningGroupByTester(SynapseSqlGlobalState state) {
        super(state);
        SynapseSqlErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setGroupByExpressions(select.getFetchColumns());
        select.setWhereClause(null);
        String originalQueryString = SynapseSqlToStringVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setWhereClause(predicate);
        String firstQueryString = SynapseSqlToStringVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = SynapseSqlToStringVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = SynapseSqlToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSetNoDuplicates(firstQueryString,
                secondQueryString, thirdQueryString, combinedString, true, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    List<Node<SynapseSqlExpression>> generateFetchColumns() {
        return Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                .map(c -> new ColumnReferenceNode<SynapseSqlExpression, SynapseSqlColumn>(c)).collect(Collectors.toList());
    }

}
