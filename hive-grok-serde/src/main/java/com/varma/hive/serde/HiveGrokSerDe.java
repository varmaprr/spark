package com.varma.hive.serde;

import com.google.common.io.Resources;
import io.krakens.grok.api.Grok;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.codehaus.jackson.map.ObjectMapper;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by varma on 7/7/2018.
 * reference: https://github.com/cloudera/cdh-twitter-example/blob/master/hive-serdes/pom.xml
 */
public class HiveGrokSerDe extends AbstractSerDe {

    private Logger LOG = LoggerFactory.getLogger(HiveGrokSerDe.class);

    private List<String> colNames;
    private StructTypeInfo rowTypeInfo;
    private ObjectInspector rowOI;
    private List<Object> row = new ArrayList<>();

    public void initialize(Configuration configuration, Properties properties) throws SerDeException {

        LOG.info("Varma: initialize...!!!! ");
        String colNamesStr = properties.getProperty(serdeConstants.LIST_COLUMNS);
        colNames = Arrays.asList(colNamesStr.split(","));

        String colTypesStr = properties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        List<TypeInfo> colTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);
        rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
        rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
        LOG.info("Varma: initialize completed !!! ");
    }

    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    public Writable serialize(Object obj, ObjectInspector oi) throws SerDeException {
        return new Text(obj.toString());
    }

    public Object deserialize(Writable blob) throws SerDeException {
        row.clear();
        LOG.info("Varma: deserialize...!!!! ");
        //row.add(blob.toString());
        try {
            parseMessage(blob.toString());
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return row;
    }

    public void parseMessage(String msg) throws IOException {
        LOG.info("Varma: parse message ...!!!! ");
        GrokCompiler compiler = GrokCompiler.newInstance();
        compiler.registerDefaultPatterns();
        compiler.register(Resources.getResource("sample-grok.pattern").openStream());
        compiler.register("foo", "%{COMMONAPACHELOG}");

        //LOG.debug("Source Message : " + record.toString())

        Grok grok = compiler.compile("%{foo}");
        Match match = grok.match(msg.toString());
        Map<String, Object> capture = match.capture();

        // Lowercase the keys as expected by hive
        Map<String, Object> lowerRoot = new HashMap();
        for (Map.Entry entry : capture.entrySet()) {
            lowerRoot.put(((String) entry.getKey()).toLowerCase(), entry.getValue());
        }
        capture = lowerRoot;

        Object value = null;
        for (String fieldName : rowTypeInfo.getAllStructFieldNames()) {
            try {
                TypeInfo fieldTypeInfo = rowTypeInfo.getStructFieldTypeInfo(fieldName);
                value = parseField(capture.get(fieldName.toLowerCase()), fieldTypeInfo);
            } catch (Exception e) {
                value = null;
            }
            row.add(value);
        }

        //LOG.debug("Grok parse result : " + resultsVector.toSeq)
        //row.add(capture);
        //Row.fromSeq(resultsVector.toSeq)
    }

    private Object parseField(Object field, TypeInfo fieldTypeInfo) {
        switch (fieldTypeInfo.getCategory()) {
            case PRIMITIVE:
                if (field instanceof String) {
                    field = field.toString().replaceAll("\n", "\\\\n");
                }
                return field;
            default:
                if (field instanceof String) {
                    field = field.toString().replaceAll("\n", "\\\\n");
                }
                return field;
        }
    }


    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowOI;
    }

    public SerDeStats getSerDeStats() {
        return null;
    }
}
