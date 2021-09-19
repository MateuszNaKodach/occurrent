package org.occurrent.eventstore.sql.spring.reactor;

import org.occurrent.condition.Condition;
import org.occurrent.filter.Filter;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

class FilterConverter {

  //TODO: What about time representation and filedNamePrefix?
  public static String convertFilterToWhereClause(Filter filter) {
    String fieldNamePrefix = null;
    requireNonNull(filter, "Filter cannot be null");

    String sql = convertFilterToSql(fieldNamePrefix, filter);
    if (sql.trim().equals("")) {
      return "";
    } else {
      return " WHERE " + sql;
    }
  }

  private static String convertFilterToSql(String fieldNamePrefix, Filter filter) {
    if (filter instanceof Filter.All) {
      return "";
    } else if (filter instanceof Filter.SingleConditionFilter) {
      Filter.SingleConditionFilter scf = (Filter.SingleConditionFilter) filter;
      String fieldName = fieldNameOf(fieldNamePrefix, scf.fieldName);
      Condition<?> conditionToUse = scf.condition;
      return convertConditionToCriteria(fieldName, conditionToUse);
    } else if (filter instanceof Filter.CompositionFilter) {
      Filter.CompositionFilter cf = (Filter.CompositionFilter) filter;
      String sqlOperator = toSqlOperator(cf);
      return cf.filters.stream()
          .map(f -> convertFilterToSql(fieldNamePrefix, f))
          .collect(Collectors.joining(sqlOperator));
    }
    return "";
  }

  private static String toSqlOperator(Filter.CompositionFilter cf) {
    switch (cf.operator) {
      case AND:
        return " AND ";
      case OR:
        return " OR ";
      default:
        throw new IllegalStateException("Unexpected value: " + cf.operator);
    }
  }

  private static String fieldNameOf(String fieldNamePrefix, String fieldName) {
    return fieldNamePrefix == null ? fieldName : fieldNamePrefix + "." + fieldName;
  }

  private static <T> String convertConditionToCriteria(String fieldName, Condition<T> condition) {
    if (condition instanceof Condition.MultiOperandCondition) {
      Condition.MultiOperandCondition<T> operation = (Condition.MultiOperandCondition<T>) condition;
      Condition.MultiOperandConditionName operationName = operation.operationName;
      List<Condition<T>> operations = operation.operations;
      String operationSql = operationNameToSql(operationName);
      return operations.stream()
          .map(c -> convertConditionToCriteria(fieldName, c))
          .collect(Collectors.joining(operationSql));
    } else if (condition instanceof Condition.SingleOperandCondition) {
      Condition.SingleOperandCondition<T> singleOperandCondition = (Condition.SingleOperandCondition<T>) condition;
      T value = singleOperandCondition.operand;
      Condition.SingleOperandConditionName singleOperandConditionName = singleOperandCondition.singleOperandConditionName;
      switch (singleOperandConditionName) {
        case EQ:
          return fieldName + " = " + "'" + value + "'";
        case LT:
          return fieldName + " < " + "'" + value + "'";
        case GT:
          return fieldName + " > " + "'" + value + "'";
        case LTE:
          return fieldName + " <= " + "'" + value + "'";
        case GTE:
          return fieldName + " >= " + "'" + value + "'";
        case NE:
          return fieldName + " != " + "'" + value + "'";
        default:
          throw new IllegalStateException("Unexpected value: " + singleOperandConditionName);
      }
    } else {
      throw new IllegalArgumentException("Unsupported condition: " + condition.getClass());
    }
  }

  private static String operationNameToSql(Condition.MultiOperandConditionName operationName) {
    switch (operationName) {
      case AND:
        return " AND ";
      case OR:
        return " OR ";
      case NOT:
        return " NOT ";
      default:
        throw new IllegalStateException("Unexpected value: " + operationName);
    }
  }
}
