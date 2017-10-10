package com.spark2.cassandra.export

import scala.beans.BeanProperty
class CassandraYaml {
  @BeanProperty var cassandra_table_export = new java.util.ArrayList[YamlProps]()
}
