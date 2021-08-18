package sqlancer.synapsesql.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.oracle.TestOracle;
import sqlancer.synapsesql.SynapseSqlErrors;
import sqlancer.synapsesql.SynapseSqlProvider.SynapseSqlGlobalState;
import sqlancer.synapsesql.SynapseSqlToStringVisitor;
import sqlancer.synapsesql.ast.SynapseSqlExpression;

public class SynapseSqlQueryPartitioningHavingTester extends SynapseSqlQueryPartitioningBase implements TestOracle {

    public SynapseSqlQueryPartitioningHavingTester(SynapseSqlGlobalState state) {
        super(state);
        SynapseSqlErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        boolean orderBy = Randomly.getBoolean();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        select.setHavingClause(null);
        String originalQueryString = SynapseSqlToStringVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setHavingClause(predicate);
        String firstQueryString = SynapseSqlToStringVisitor.asString(select);
        select.setHavingClause(negatedPredicate);
        String secondQueryString = SynapseSqlToStringVisitor.asString(select);
        select.setHavingClause(isNullPredicate);
        String thirdQueryString = SynapseSqlToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    protected Node<SynapseSqlExpression> generatePredicate() {
        return gen.generateHavingClause();
    }

    @Override
    List<Node<SynapseSqlExpression>> generateFetchColumns() {
        return Arrays.asList(gen.generateHavingClause());
    }

}
