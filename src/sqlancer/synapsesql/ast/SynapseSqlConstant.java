package sqlancer.synapsesql.ast;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import sqlancer.common.ast.newast.Node;

public class SynapseSqlConstant implements Node<SynapseSqlExpression> {

    private SynapseSqlConstant() {
    }

    public static class SynapseSqlNullConstant extends SynapseSqlConstant {

        @Override
        public String toString() {
            return null;
        }

    }

    public static class SynapseSqlIntConstant extends SynapseSqlConstant {

        private final long value;

        public SynapseSqlIntConstant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public long getValue() {
            return value;
        }

    }

    public static class SynapseSqlDoubleConstant extends SynapseSqlConstant {

        private final double value;

        public SynapseSqlDoubleConstant(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "'+Inf'";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "'-Inf'";
            }
            return String.valueOf(value);
        }

    }

    public static class SynapseSqlTextConstant extends SynapseSqlConstant {

        private final String value;

        public SynapseSqlTextConstant(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "'" + value.replace("'", "''") + "'";
        }

    }

    public static class SynapseSqlBitConstant extends SynapseSqlConstant {

        private final String value;

        public SynapseSqlBitConstant(long value) {
            this.value = Long.toBinaryString(value);
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "B'" + value + "'";
        }

    }

    public static class SynapseSqlDateConstant extends SynapseSqlConstant {

        public String textRepr;

        public SynapseSqlDateConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("DATE '%s'", textRepr);
        }

    }

    public static class SynapseSqlTimestampConstant extends SynapseSqlConstant {

        public String textRepr;

        public SynapseSqlTimestampConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("TIMESTAMP '%s'", textRepr);
        }

    }

    public static class SynapseSqlBooleanConstant extends SynapseSqlConstant {

        private final boolean value;

        public SynapseSqlBooleanConstant(boolean value) {
            this.value = value;
        }

        public boolean getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

    }

    public static Node<SynapseSqlExpression> createStringConstant(String text) {
        return new SynapseSqlTextConstant(text);
    }

    public static Node<SynapseSqlExpression> createFloatConstant(double val) {
        return new SynapseSqlDoubleConstant(val);
    }

    public static Node<SynapseSqlExpression> createIntConstant(long val) {
        return new SynapseSqlIntConstant(val);
    }

    public static Node<SynapseSqlExpression> createNullConstant() {
        return new SynapseSqlNullConstant();
    }

    public static Node<SynapseSqlExpression> createBooleanConstant(boolean val) {
        return new SynapseSqlBooleanConstant(val);
    }

    public static Node<SynapseSqlExpression> createDateConstant(long integer) {
        return new SynapseSqlDateConstant(integer);
    }

    public static Node<SynapseSqlExpression> createTimestampConstant(long integer) {
        return new SynapseSqlTimestampConstant(integer);
    }

}
