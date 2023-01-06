package com.slack.kaldb.logstore.calcite;

import java.util.LinkedHashMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Table representing an Apache Lucene index.
 *
 * <p>The table implements the {@link ScannableTable} interface and knows how to extract rows from
 * Lucene and map them to Calcite's internal representation. Implementing this interface makes it
 * easy to run queries over Lucene without introducing custom rules and operators.
 */
public final class LuceneTable extends AbstractTable implements ScannableTable {
  private final String indexPath;
  private final RelDataType dataType;

  public LuceneTable(String indexPath, RelDataType dataType) {
    this.indexPath = indexPath;
    this.dataType = dataType;
  }

  @Override
  public Enumerable<Object[]> scan(final DataContext root) {
    LinkedHashMap<String, SqlTypeName> fields = new LinkedHashMap<>();
    for (RelDataTypeField f : dataType.getFieldList()) {
      fields.put(f.getName(), f.getType().getSqlTypeName());
    }
    return new LuceneEnumerable(indexPath, fields, "*:*");
  }

  @Override
  public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
    return typeFactory.copyType(dataType);
  }

  /** Returns the path to the index in the filesystem. */
  public String indexPath() {
    return indexPath;
  }
}
