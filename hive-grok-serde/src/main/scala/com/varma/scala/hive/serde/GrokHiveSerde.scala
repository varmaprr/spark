package com.varma.scala.hive.serde

import java.util
import java.util.Properties

import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.typeinfo.{StructTypeInfo, TypeInfo, TypeInfoFactory, TypeInfoUtils}
import org.apache.hadoop.hive.serde2.{AbstractSerDe, SerDe, SerDeStats}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.Text

/**
  * Created by varma on 7/7/2018.
  */
class GrokHiveSerde extends SerDe{

  private var colNames:List[String] = null
  private var rowTypeInfo:StructTypeInfo = null
  private var rowOI:ObjectInspector = null
  private val row = new util.ArrayList[AnyRef]

  override def getSerializedClass: Class[_ <: Writable] = classOf[Text];

  override def getSerDeStats: SerDeStats = null

  override def getObjectInspector: ObjectInspector = rowOI

  override def initialize(configuration: Configuration, properties: Properties): Unit = {
    val colNamesStr: String = properties.getProperty(serdeConstants.LIST_COLUMNS)
    colNames = colNamesStr.split(",").toList;

    val colTypesStr: String = properties.getProperty(serdeConstants.LIST_COLUMN_TYPES)
    val colTypes: util.ArrayList[TypeInfo] = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr)
    rowTypeInfo = TypeInfoFactory.getStructTypeInfo(colNames, colTypes).asInstanceOf[StructTypeInfo]
    rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo)
  }

  override def serialize(o: scala.Any, objectInspector: ObjectInspector): Writable = new Text(o.toString)

  override def deserialize(writable: Writable): AnyRef = {
    row.clear()
    row.add(writable.toString)
    row
  }
}
