package sqlancer.h2;

import sqlancer.common.ast.newast.Node;

public class H2Constant implements Node<H2Expression> {

    private H2Constant() {
    }

    public static class H2NullConstant extends H2Constant {

        @Override
        public String toString() {
            return "NULL";
        }

    }

    public static class H2IntConstant extends H2Constant {

        private final long value;

        public H2IntConstant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

    }

    public static class H2BoolConstant extends H2Constant {

        private final boolean value;

        public H2BoolConstant(boolean value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

    }

    public static class H2StringConstant extends H2Constant {

        private final String value;

        public H2StringConstant(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("'%s'", value);
        }

    }

    public static class H2DoubleConstant extends H2Constant {

        private final double value;

        public H2DoubleConstant(double value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

    }

    public static Node<H2Expression> createIntConstant(long val) {
        return new H2IntConstant(val);
    }

    public static Node<H2Expression> createNullConstant() {
        return new H2NullConstant();
    }

    public static Node<H2Expression> createBoolConstant(boolean val) {
        return new H2BoolConstant(val);
    }

    public static Node<H2Expression> createStringConstant(String val) {
        return new H2StringConstant(val);
    }

    public static Node<H2Expression> createDoubleConstant(double val) {
        return new H2DoubleConstant(val);
    }

}
