package io.mybatis.mapper.optimistic;

import io.mybatis.mapper.example.ExampleProvider;
import io.mybatis.mapper.logical.LogicalColumn;
import io.mybatis.mapper.logical.LogicalProvider;
import io.mybatis.provider.EntityColumn;
import io.mybatis.provider.EntityTable;
import io.mybatis.provider.SqlScript;
import org.apache.ibatis.builder.annotation.ProviderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author PING
 * @version 1.0
 * @date 2025/1/23 11:26
 */
public class OptimisticProvider {

  private static final Logger log = LoggerFactory.getLogger(OptimisticProvider.class);

  public interface OptimisticSqlScript extends SqlScript {
    default String logicalNotEqualCondition(EntityTable entity) {
      EntityColumn logicalColumn = getLogicalColumn(entity);
      if (Objects.isNull(logicalColumn)) {
        return "";
      }
      return " AND " + LogicalProvider.columnNotEqualsValueCondition(logicalColumn, LogicalProvider.deleteValue(logicalColumn)) + LF;
    }
  }

  /**
   * 根据主键更新实体中不为空的字段，强制字段不区分是否null，都更新
   *
   * @param providerContext 上下文
   * @return cacheKey
   */
  public static String updateByPrimaryKeySelectiveWithForceFields(ProviderContext providerContext) {
    return SqlScript.caching(providerContext, new OptimisticSqlScript() {

      @Override
      public String getSql(EntityTable entity) {
        return "UPDATE " + entity.tableName()
            + set(() ->
            entity.updateColumns().stream().map(column -> {

                  if (isVersionColumn(column)) {
                    return columnEqualsPropertyVersion(column);
                  }

                  return
                      choose(() -> {
                        return whenTest("fns != null and fns.fieldNames().contains('" + column.property() + "')", () -> column.columnEqualsProperty("entity.") + ",")
                            + whenTest(column.notNullTest("entity."), () -> column.columnEqualsProperty("entity.") + ",");
                      });
                }
            ).collect(Collectors.joining(LF)))
            + where(() -> {
          List<EntityColumn> entityColumns = entity.idColumns();
          // 如果版本号字段 !=null 那么增加进去
          EntityColumn optimistic = isOptimistic(entity.updateColumns());
          if (optimistic != null) {
            entityColumns.add(optimistic);
          }
          return entityColumns.stream().map(column -> column.columnEqualsProperty("entity.")).collect(Collectors.joining(" AND "));
        })
            + logicalNotEqualCondition(entity);
      }
    });
  }


  /**
   * 根据主键逻辑删除 不包含逻辑字段就会使用delete
   *
   * @param providerContext 上下文
   * @return cacheKey
   */
  public static String deleteByPrimaryKey(ProviderContext providerContext) {

    return SqlScript.caching(providerContext, new OptimisticSqlScript() {
          @Override
          public String getSql(EntityTable entity) {
            EntityColumn logicColumn = getLogicalColumn(entity);

//            List<EntityColumn> entityColumns = entity.idColumns();
//            // 如果版本号字段 !=null 那么增加进去
//            EntityColumn optimistic = isOptimistic(entity.updateColumns());
//            if (optimistic != null) {
//              entityColumns.add(optimistic);
//            }

            if (logicColumn != null) {
              return "UPDATE " + entity.tableName()
                  + " SET " + LogicalProvider.columnEqualsValue(logicColumn, LogicalProvider.deleteValue(logicColumn))
//                  + (optimistic != null ? optimistic.columnEqualsProperty() + " + 1" : "")
                  + " WHERE " + entity.idColumns().stream().map(EntityColumn::columnEqualsProperty).collect(Collectors.joining(" AND "))
                  + logicalNotEqualCondition(entity);
            }
            return "DELETE FROM " + entity.tableName()
                + " WHERE " + entity.idColumns().stream().map(EntityColumn::columnEqualsProperty).collect(Collectors.joining(" AND "));
          }
        }
    );
  }

  /**
   * 根据实体信息批量删除
   *
   * @param providerContext 上下文
   * @return cacheKey
   */
  public static String delete(ProviderContext providerContext) {
    return SqlScript.caching(providerContext, new OptimisticSqlScript() {
      @Override
      public String getSql(EntityTable entity) {
        EntityColumn logicColumn = getLogicalColumn(entity);
        if (logicColumn != null) {
          return "UPDATE " + entity.tableName()
              + " SET " + LogicalProvider.columnEqualsValue(logicColumn, LogicalProvider.deleteValue(logicColumn))
              + parameterNotNull("Parameter cannot be null")
              + where(() -> entity.columns().stream()
              .map(column -> ifTest(column.notNullTest(), () -> "AND " + column.columnEqualsProperty()))
              .collect(Collectors.joining(LF)) + logicalNotEqualCondition(entity));
        }

        return "DELETE FROM " + entity.tableName()
            + parameterNotNull("Parameter cannot be null")
            + where(() ->
            entity.columns().stream().map(column ->
                ifTest(column.notNullTest(), () -> "AND " + column.columnEqualsProperty())
            ).collect(Collectors.joining(LF)));
      }
    });
  }

  /**
   * 根据主键更新实体
   *
   * @param providerContext 上下文
   * @return cacheKey
   */
  public static String updateByPrimaryKey(ProviderContext providerContext) {
    return SqlScript.caching(providerContext, new OptimisticSqlScript() {
      @Override
      public String getSql(EntityTable entity) {

        List<EntityColumn> entityColumns = entity.idColumns();
        // 如果版本号字段 !=null 那么增加进去
        EntityColumn optimistic = isOptimistic(entity.updateColumns());
        if (optimistic != null) {
          entityColumns.add(optimistic);
        }

        return "UPDATE " + entity.tableName()
            + " SET " + entity.updateColumns().stream().map(column -> {
          if (isVersionColumn(column)) {
            return columnEqualsPropertyVersion(column);
          }
          return column.columnEqualsProperty();
        }).collect(Collectors.joining(","))
            + where(() -> entityColumns.stream().map(EntityColumn::columnEqualsProperty).collect(Collectors.joining(" AND ")))
            + logicalNotEqualCondition(entity);
      }
    });
  }

  /**
   * 根据主键更新实体中不为空的字段
   *
   * @param providerContext 上下文
   * @return cacheKey
   */
  public static String updateByPrimaryKeySelective(ProviderContext providerContext) {
    return SqlScript.caching(providerContext, new OptimisticSqlScript() {
      @Override
      public String getSql(EntityTable entity) {

        List<EntityColumn> entityColumns = entity.idColumns();
        // 如果版本号字段 !=null 那么增加进去
        EntityColumn optimistic = isOptimistic(entity.updateColumns());
        if (optimistic != null) {
          entityColumns.add(optimistic);
        }
        return "UPDATE " + entity.tableName()
            + set(() ->
            entity.updateColumns().stream().map(column -> {
                  if (isVersionColumn(column)) {
                    return columnEqualsPropertyVersion(column);
                  }
                  return ifTest(column.notNullTest(), () -> column.columnEqualsProperty() + ",");
                }
            ).collect(Collectors.joining(LF)))
            + where(() -> entityColumns.stream().map(EntityColumn::columnEqualsProperty).collect(Collectors.joining(" AND ")))
            + logicalNotEqualCondition(entity);
      }
    });
  }


  /**
   * 根据 Example 条件批量更新实体信息
   *
   * @param providerContext 上下文
   * @return cacheKey
   */
  public static String updateByExample(ProviderContext providerContext) {
    return SqlScript.caching(providerContext, new OptimisticSqlScript() {
      @Override
      public String getSql(EntityTable entity) {

        EntityColumn optimistic = isOptimistic(entity.updateColumns());

        return ifTest("example.startSql != null and example.startSql != ''", () -> "${example.startSql}")
            + "UPDATE " + entity.tableName()
            + set(() -> entity.updateColumns().stream().map(column -> {
          if (isVersionColumn(column)) {
            return columnEqualsPropertyVersion(column);
          }
          return column.columnEqualsProperty("entity.");
        }).collect(Collectors.joining(",")))
            + variableNotNull("example", "Example cannot be null")
            //是否允许空条件，默认允许，允许时不检查查询条件
            + (entity.getPropBoolean("updateByExample.allowEmpty", true) ?
            "" : variableIsFalse("example.isEmpty()", "Example Criteria cannot be empty"))
            + trim("WHERE", "", "WHERE |OR |AND ", "", () -> {
          String whereSql = ExampleProvider.UPDATE_BY_EXAMPLE_WHERE_CLAUSE + logicalNotEqualCondition(entity);
          if (optimistic != null) {
            whereSql = whereSql + " AND " + optimistic.columnEqualsProperty("entity.");
          }
          return whereSql;
        })
            + ifTest("example.endSql != null and example.endSql != ''", () -> "${example.endSql}");
      }
    });
  }

  /**
   * 根据 Example 条件批量更新实体不为空的字段
   *
   * @param providerContext 上下文
   * @return cacheKey
   */
  public static String updateByExampleSelective(ProviderContext providerContext) {
    return SqlScript.caching(providerContext, new OptimisticSqlScript() {
      @Override
      public String getSql(EntityTable entity) {

        EntityColumn optimistic = isOptimistic(entity.updateColumns());

        return ifTest("example.startSql != null and example.startSql != ''", () -> "${example.startSql}")
            + "UPDATE " + entity.tableName()
            + set(() -> entity.updateColumns().stream().map(
            column -> {
              if (isVersionColumn(column)) {
                return columnEqualsPropertyVersion(column);
              }
              return ifTest(column.notNullTest("entity."), () -> column.columnEqualsProperty("entity.") + ",");
            }).collect(Collectors.joining(LF)))
            + variableNotNull("example", "Example cannot be null")
            //是否允许空条件，默认允许，允许时不检查查询条件
            + (entity.getPropBoolean("updateByExampleSelective.allowEmpty", true) ?
            "" : variableIsFalse("example.isEmpty()", "Example Criteria cannot be empty"))
            + trim("WHERE", "", "WHERE |OR |AND ", "", () -> {
          String whereSql = ExampleProvider.UPDATE_BY_EXAMPLE_WHERE_CLAUSE + logicalNotEqualCondition(entity);
          if (optimistic != null) {
            whereSql = whereSql + " AND " + optimistic.columnEqualsProperty("entity.");
          }
          return whereSql;
        })
            + ifTest("example.endSql != null and example.endSql != ''", () -> "${example.endSql}");
      }
    });
  }


  /**
   * 返回version的update字段
   * update xxx set version = version + 1
   *
   * @param column
   * @return
   */
  private static String columnEqualsPropertyVersion(EntityColumn column) {
    return column.column() + " = " + column.column() + " + 1";
  }

  /**
   * 判断是否包含version注解
   *
   * @param versionColumn 字段
   * @return
   */
  private static boolean isVersionColumn(EntityColumn versionColumn) {
    Annotation[] annotations = versionColumn.field().getAnnotations();
    for (Annotation annotation : annotations) {
      log.info("annotation.annotationType().getName() {}", annotation.annotationType().getName());
      return annotation.annotationType().getName().equals("jakarta.persistence.Version");
    }
    return false;
  }

  private static EntityColumn isOptimistic(List<EntityColumn> updateColumns) {
    for (EntityColumn updateColumn : updateColumns) {
      if (isVersionColumn(updateColumn)) {
        return updateColumn;
      }
    }
    return null;
  }

  public static EntityColumn getLogicalColumn(EntityTable entity) {
    List<EntityColumn> logicColumns = entity.columns().stream().filter(c -> c.field().isAnnotationPresent(LogicalColumn.class)).collect(Collectors.toList());
    if (logicColumns.size() != 1) {
      log.warn("There are no or multiple fields marked with @LogicalColumn {}", entity.tableName());
      return null;
    }
    return logicColumns.get(0);
  }

}
