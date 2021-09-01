package sqlancer.synapsesql.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewBetweenOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewCaseOperatorNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.NewInOperatorNode;
import sqlancer.common.ast.newast.NewOrderingTerm;
import sqlancer.common.ast.newast.NewOrderingTerm.Ordering;
import sqlancer.common.ast.newast.NewTernaryNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.synapsesql.SynapseSqlProvider.SynapseSqlGlobalState;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlColumn;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlCompositeDataType;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlDataType;
import sqlancer.synapsesql.ast.SynapseSqlConstant;
import sqlancer.synapsesql.ast.SynapseSqlExpression;

public final class SynapseSqlExpressionGenerator extends UntypedExpressionGenerator<Node<SynapseSqlExpression>, SynapseSqlColumn> {

    private final SynapseSqlGlobalState globalState;

    public SynapseSqlExpressionGenerator(SynapseSqlGlobalState globalState) {
        this.globalState = globalState;
    }

    private enum Expression {
        UNARY_POSTFIX, UNARY_PREFIX, BINARY_COMPARISON, BINARY_LOGICAL, BINARY_ARITHMETIC, CAST, FUNC, BETWEEN, CASE,
        IN, COLLATE, LIKE_ESCAPE
    }

    @Override
    protected Node<SynapseSqlExpression> generateExpression(int depth) {
        return generateExpression(depth, SynapseSqlDataType.BOOLEAN);
    }

    private Node<SynapseSqlExpression> generateExpression(int depth, SynapseSqlDataType type) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }
        if (allowAggregates && Randomly.getBoolean()) {
            SynapseSqlAggregateFunction aggregate = SynapseSqlAggregateFunction.getRandom();
            allowAggregates = false;
            return new NewFunctionNode<>(generateExpressions(depth + 1, aggregate.getNrArgs()), aggregate);
        }
        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
        if (!globalState.getDbmsSpecificOptions().testCollate) {
            possibleOptions.remove(Expression.COLLATE);
            possibleOptions.remove(Expression.LIKE_ESCAPE);
        }
        if (!globalState.getDbmsSpecificOptions().testFunctions) {
            possibleOptions.remove(Expression.FUNC);
        }
        if (!globalState.getDbmsSpecificOptions().testCasts) {
            possibleOptions.remove(Expression.CAST);
        }
        if (!globalState.getDbmsSpecificOptions().testBetween) {
            possibleOptions.remove(Expression.BETWEEN);
        }
        if (!globalState.getDbmsSpecificOptions().testIn) {
            possibleOptions.remove(Expression.IN);
        }
        if (!globalState.getDbmsSpecificOptions().testCase) {
            possibleOptions.remove(Expression.CASE);
        }
        if (!globalState.getDbmsSpecificOptions().testBinaryComparisons) {
            possibleOptions.remove(Expression.BINARY_COMPARISON);
        }
        if (!globalState.getDbmsSpecificOptions().testBinaryLogicals) {
            possibleOptions.remove(Expression.BINARY_LOGICAL);
        }
        Expression expr = Randomly.fromList(possibleOptions);
        switch (expr) {
        case COLLATE:
            return new NewUnaryPostfixOperatorNode<SynapseSqlExpression>(generateExpression(depth + 1),
                    SynapseSqlCollate.getRandom());
        case UNARY_PREFIX:
            return new NewUnaryPrefixOperatorNode<SynapseSqlExpression>(generateExpression(depth + 1),
                    SynapseSqlUnaryPrefixOperator.getRandom());
        case UNARY_POSTFIX:
            return new NewUnaryPostfixOperatorNode<SynapseSqlExpression>(generateExpression(depth + 1),
                    SynapseSqlUnaryPostfixOperator.getRandom());
        case BINARY_COMPARISON:
            Operator op = SynapseSqlBinaryComparisonOperator.getRandom();
            return new NewBinaryOperatorNode<SynapseSqlExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), op);
        case BINARY_LOGICAL:
            op = SynapseSqlBinaryLogicalOperator.getRandom();
            return new NewBinaryOperatorNode<SynapseSqlExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), op);
        case BINARY_ARITHMETIC:
            return new NewBinaryOperatorNode<SynapseSqlExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), SynapseSqlBinaryArithmeticOperator.getRandom());
        case CAST:
            return new SynapseSqlCastOperation(generateExpression(depth + 1), SynapseSqlCompositeDataType.getRandom(this.globalState.getRandomly()));
        case FUNC:
            DBFunction func = DBFunction.getRandom();
            return new NewFunctionNode<SynapseSqlExpression, DBFunction>(generateExpressions(func.getNrArgs()), func);
        case BETWEEN:
            return new NewBetweenOperatorNode<SynapseSqlExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), generateExpression(depth + 1), Randomly.getBoolean());
        case IN:
            return new NewInOperatorNode<SynapseSqlExpression>(generateExpression(depth + 1),
                    generateExpressions(depth + 1, Randomly.smallNumber() + 1), Randomly.getBoolean());
        case CASE:
            int nr = Randomly.smallNumber() + 1;
            return new NewCaseOperatorNode<SynapseSqlExpression>(generateExpression(depth + 1),
                    generateExpressions(depth + 1, nr), generateExpressions(depth + 1, nr),
                    generateExpression(depth + 1));
        case LIKE_ESCAPE:
            return new NewTernaryNode<SynapseSqlExpression>(generateExpression(depth + 1), generateExpression(depth + 1),
                    generateExpression(depth + 1), "LIKE", "ESCAPE");
        default:
            throw new AssertionError();
        }
    }

    @Override
    protected Node<SynapseSqlExpression> generateColumn() {
        SynapseSqlColumn column = Randomly.fromList(columns);
        return new ColumnReferenceNode<SynapseSqlExpression, SynapseSqlColumn>(column);
    }

    public Node<SynapseSqlExpression> generateConstant(SynapseSqlCompositeDataType compositeType){

        SynapseSqlDataType type = compositeType.getPrimitiveDataType();
        switch (type) {
        case INT:
            if (!globalState.getDbmsSpecificOptions().testIntConstants) {
                throw new IgnoreMeException();
            }

            int mask = 1;
            for(int i = 1; i<compositeType.getSize(); i++) {
                mask = mask << 1;
                mask = mask | 1;
            }
            return SynapseSqlConstant.createIntConstant(globalState.getRandomly().getInteger() & mask);
        case DATE:
            if (!globalState.getDbmsSpecificOptions().testDateConstants) {
                throw new IgnoreMeException();
            }
            return SynapseSqlConstant.createDateConstant(globalState.getRandomly().getInteger());
        case TIMESTAMP:
            if (!globalState.getDbmsSpecificOptions().testTimestampConstants) {
                throw new IgnoreMeException();
            }
            return SynapseSqlConstant.createTimestampConstant(globalState.getRandomly().getInteger());
        case VARCHAR:
            if (!globalState.getDbmsSpecificOptions().testStringConstants) {
                throw new IgnoreMeException();
            }
            return SynapseSqlConstant.createStringConstant(globalState.getRandomly().getString());
        case BOOLEAN:
            if (!globalState.getDbmsSpecificOptions().testBooleanConstants) {
                throw new IgnoreMeException();
            }
            return SynapseSqlConstant.createBooleanConstant(Randomly.getBoolean());
        case FLOAT:
            if (!globalState.getDbmsSpecificOptions().testFloatConstants) {
                throw new IgnoreMeException();
            }
            return SynapseSqlConstant.createFloatConstant(globalState.getRandomly().getDouble());
        default:
            throw new AssertionError();
        }
    }

    @Override
    public Node<SynapseSqlExpression> generateConstant() {
        if (Randomly.getBooleanWithSmallProbability()) {
            return SynapseSqlConstant.createNullConstant();
        }
        SynapseSqlCompositeDataType type = SynapseSqlCompositeDataType.getRandom(globalState.getRandomly());
        return generateConstant(type);
    }

    @Override
    public List<Node<SynapseSqlExpression>> generateOrderBys() {
        List<Node<SynapseSqlExpression>> expr = super.generateOrderBys();
        List<Node<SynapseSqlExpression>> newExpr = new ArrayList<>(expr.size());
        for (Node<SynapseSqlExpression> curExpr : expr) {
            if (Randomly.getBoolean()) {
                curExpr = new NewOrderingTerm<>(curExpr, Ordering.getRandom());
            }
            newExpr.add(curExpr);
        }
        return newExpr;
    };

    public static class SynapseSqlCastOperation extends NewUnaryPostfixOperatorNode<SynapseSqlExpression> {

        public SynapseSqlCastOperation(Node<SynapseSqlExpression> expr, SynapseSqlCompositeDataType type) {
            super(expr, new Operator() {

                @Override
                public String getTextRepresentation() {
                    return "::" + type.toString();
                }
            });
        }

    }

    public enum SynapseSqlAggregateFunction {
        MAX(1), MIN(1), AVG(1), COUNT(1), STRING_AGG(1), FIRST(1), SUM(1), STDDEV_SAMP(1), STDDEV_POP(1), VAR_POP(1),
        VAR_SAMP(1), COVAR_POP(1), COVAR_SAMP(1);

        private int nrArgs;

        SynapseSqlAggregateFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        public static SynapseSqlAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }

    }

    public enum DBFunction {
        // trigonometric functions
        ACOS(1), //
        ASIN(1), //
        ATAN(1), //
        COS(1), //
        SIN(1), //
        TAN(1), //
        COT(1), //
        ATAN2(1), //
        // math functions
        ABS(1), //
        CEIL(1), //
        CEILING(1), //
        FLOOR(1), //
        LOG(1), //
        LOG10(1), LOG2(1), //
        LN(1), //
        PI(0), //
        SQRT(1), //
        POWER(1), //
        CBRT(1), //
        ROUND(2), //
        SIGN(1), //
        DEGREES(1), //
        RADIANS(1), //
        MOD(2), //
        // string functions
        LENGTH(1), //
        LOWER(1), //
        UPPER(1), //
        SUBSTRING(3), //
        REVERSE(1), //
        CONCAT(1, true), //
        CONCAT_WS(1, true), CONTAINS(2), //
        PREFIX(2), //
        SUFFIX(2), //
        INSTR(2), //
        PRINTF(1, true), //
        REGEXP_MATCHES(2), //
        REGEXP_REPLACE(3), //
        STRIP_ACCENTS(1), //

        // date functions
        DATE_PART(2), AGE(2),

        COALESCE(3), NULLIF(2),

        // LPAD(3),
        // RPAD(3),
        LTRIM(1), RTRIM(1),
        // LEFT(2), https://github.com/cwida/duckdb/issues/633
        // REPEAT(2),
        REPLACE(3), UNICODE(1),

        BIT_COUNT(1), BIT_LENGTH(1), LAST_DAY(1), MONTHNAME(1), DAYNAME(1), YEARWEEK(1), DAYOFMONTH(1), WEEKDAY(1),
        WEEKOFYEAR(1),

        IFNULL(2), IF(3);

        private int nrArgs;
        private boolean isVariadic;

        DBFunction(int nrArgs) {
            this(nrArgs, false);
        }

        DBFunction(int nrArgs, boolean isVariadic) {
            this.nrArgs = nrArgs;
            this.isVariadic = isVariadic;
        }

        public static DBFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            if (isVariadic) {
                return Randomly.smallNumber() + nrArgs;
            } else {
                return nrArgs;
            }
        }

    }

    public enum SynapseSqlUnaryPostfixOperator implements Operator {

        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

        private String textRepr;

        SynapseSqlUnaryPostfixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static SynapseSqlUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public static final class SynapseSqlCollate implements Operator {

        private final String textRepr;

        private SynapseSqlCollate(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return "COLLATE " + textRepr;
        }

        public static SynapseSqlCollate getRandom() {
            return new SynapseSqlCollate(SynapseSqlTableGenerator.getRandomCollate());
        }

    }

    public enum SynapseSqlUnaryPrefixOperator implements Operator {

        NOT("NOT"), PLUS("+"), MINUS("-");

        private String textRepr;

        SynapseSqlUnaryPrefixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static SynapseSqlUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum SynapseSqlBinaryLogicalOperator implements Operator {

        AND, OR;

        @Override
        public String getTextRepresentation() {
            return toString();
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum SynapseSqlBinaryArithmeticOperator implements Operator {
        CONCAT("||"), ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%"), AND("&"), OR("|"), XOR("#"), LSHIFT("<<"),
        RSHIFT(">>");

        private String textRepr;

        SynapseSqlBinaryArithmeticOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public enum SynapseSqlBinaryComparisonOperator implements Operator {

        EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!="),
        LIKE("LIKE"), NOT_LIKE("NOT LIKE");
        //SIMILAR_TO("SIMILAR TO"), NOT_SIMILAR_TO("NOT SIMILAR TO"),
        //REGEX_POSIX("~"), REGEX_POSIT_NOT("!~");

        private String textRepr;

        SynapseSqlBinaryComparisonOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public NewFunctionNode<SynapseSqlExpression, SynapseSqlAggregateFunction> generateArgsForAggregate(
            SynapseSqlAggregateFunction aggregateFunction) {
        return new NewFunctionNode<SynapseSqlExpression, SynapseSqlExpressionGenerator.SynapseSqlAggregateFunction>(
                generateExpressions(aggregateFunction.getNrArgs()), aggregateFunction);
    }

    public Node<SynapseSqlExpression> generateAggregate() {
        SynapseSqlAggregateFunction aggrFunc = SynapseSqlAggregateFunction.getRandom();
        return generateArgsForAggregate(aggrFunc);
    }

    @Override
    public Node<SynapseSqlExpression> negatePredicate(Node<SynapseSqlExpression> predicate) {
        return new NewUnaryPrefixOperatorNode<>(predicate, SynapseSqlUnaryPrefixOperator.NOT);
    }

    @Override
    public Node<SynapseSqlExpression> isNull(Node<SynapseSqlExpression> expr) {
        return new NewUnaryPostfixOperatorNode<>(expr, SynapseSqlUnaryPostfixOperator.IS_NULL);
    }

}
