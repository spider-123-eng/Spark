package com.spark2.cassandra.export

import scala.beans.BeanProperty

class YamlProps {
  @BeanProperty var table_name = ""
  @BeanProperty var keyspace = ""
  @BeanProperty var output_location = ""
  @BeanProperty var duration_in_hour = ""
}
