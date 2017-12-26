package tv.freewheel.reporting.matcher;

import com.google.common.collect.ImmutableMap;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.parquet.avro.AvroWriteSupport;

import tv.freewheel.reporting.ddl.MysqlDDL;
import tv.freewheel.reporting.ddl.TableElement;

import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
@Deprecated
public class DimensionTransformerReducer extends Reducer<Text, Text, Void, GenericRecord>
{
    private MultipleOutputs<Void, GenericRecord> output;
    // private Schema schema;
    private Map<String, Schema> schemas;
    private Map<String, List<Type>> typesMap;

    SimpleDateFormat dateFormatter;
    SimpleDateFormat dateTimeFormatter;

    @Override
    public void setup(Context context) throws IOException
    {
        Path ddlPath = new Path(context.getConfiguration().get("ddlPath"));
        FileSystem fs = FileSystem.get(context.getConfiguration());
        // ddlPath should be a directory, in which contains all the ddl files
        if (!fs.isDirectory(ddlPath)) {
            throw new RuntimeException("DDL path is not a directory or not exist:" + ddlPath.toString());
        }

        Map<String, TableElement> tableDeclares = new HashMap<>();
        // support multiple ddl files.
        for (FileStatus ddl : fs.listStatus(ddlPath)) {
            Map<String, TableElement> declares;
            if (ddl.isFile() && ddl.getPath().getName().toLowerCase().endsWith(".ddl")) {
                declares = MysqlDDL.parse(new InputStreamReader(fs.open(ddl.getPath())));
                for (String tbName : declares.keySet()) {
                    if (tableDeclares.containsKey(tbName)) {
                        throw new RuntimeException("Error: Duplicated definition for table " + tbName);
                    }
                    tableDeclares.put(tbName, declares.get(tbName));
                }
            }
        }

        ImmutableMap.Builder<String, Schema> schemasBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, List<Type>> typesBuilder = ImmutableMap.builder();
        for (String tableName : tableDeclares.keySet()) {
            List<Type> types = new ArrayList<Type>();
            List<Schema.Field> fields = new ArrayList<>();
            TableElement table = tableDeclares.get(tableName);
            for (String columnName : table.getColumnNames()) {
                String columnType = table.getColumnType(columnName);
                // Since avro doesn't support column name started with number,
                // the '4a_ad_id'@'d_creative' will be replaced with 'ad_id'.
                // 2015-06-24
                if (columnName.compareTo("4a_ad_id") == 0) {
                    columnName = "ad_id";
                }
                /*
                Types:
                 1)bigint
                 2)date
                 3)datetime
                 4)decimal
                 5)int
                 6)smallint
                 7)text
                 8)timestamp
                 9)tinyint
                10)varchar
                 */
                //use field's doc to record expected type.
                String doc = "";
                Schema type = null;
                if (columnType.toLowerCase().contains("int")) {
                    doc = "long";
                    type = Schema.create(Type.LONG);
                }
                else if (columnType.toLowerCase().contains("timestamp")) {
                    doc = "timestamp";
                    type = Schema.create(Type.LONG);
                }
                else if (columnType.toLowerCase().contains("char")) {
                    doc = "varchar";
                    type = Schema.create(Type.STRING);
                }
                else if (columnType.toLowerCase().contains("text")) {
                    doc = "varchar";
                    type = Schema.create(Type.STRING);
                }
                else if (columnType.toLowerCase().contains("date")) {
                    doc = "date";
                    type = Schema.create(Type.LONG);
                }
                else if (columnType.toLowerCase().contains("decimal")) {
                    doc = "double";
                    type = Schema.create(Type.DOUBLE);
                }
                else {
                    throw new IOException("Unsupported type: " + columnType);
                }

                types.add(type.getType());
                Schema optional = Schema.createUnion(Arrays.asList(type, Schema.create(Type.NULL)));
                fields.add(new Schema.Field(columnName, optional, doc, null));
            }

            String namespace = "tv.freewheel.reporting.schemas.dimensions";
            Schema schema = Schema.createRecord(tableName, null, namespace, false);
            schema.setFields(fields);
            schemasBuilder.put(tableName, schema);
            typesBuilder.put(tableName, types);
        }
        schemas = schemasBuilder.build();
        typesMap = typesBuilder.build();
        output = new MultipleOutputs<Void, GenericRecord>(context);

        dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
        dateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void reduce(Text id, Iterable<Text> lines, Context context) throws IOException, InterruptedException
    {
        String tableName = id.toString().split("#")[0];
        if (!schemas.containsKey(tableName)) {
            throw new RuntimeException(String.format("Table %s can't find schema definition.", tableName));
        }
        Schema schema = schemas.get(tableName);
        List<Type> types = typesMap.get(tableName);
        AvroWriteSupport.setSchema(context.getConfiguration(), schema);
        for (Text line : lines) {
            List<String> fields = split(line.toString());
            if (fields.size() != schema.getFields().size()) {
                String err = String.format("%s found malformed line: expected_field: %d, actual_field: %d, line: %s",
                        tableName, schema.getFields().size(), fields.size(), line);
                System.err.println(err);
                throw new RuntimeException(err);
            }
            GenericRecord record = new GenericData.Record(schema);
            for (int i = 0; i < fields.size(); i++) {
                String ddlType = schema.getFields().get(i).doc();
                Type type = types.get(i);
                String field = fields.get(i).equals("\\N") ? null : fields.get(i);
                try {
                    switch (type) {
                    case DOUBLE:
                        Double vd = (field == null) ? null : Double.parseDouble(field);
                        record.put(i, vd);
                        break;
                    case LONG:
                    /*
                    source type/format could be:
                      1) decimal: decimal format may occur on Long type field, like "25.000"
                      2) xxxint: 12345
                      3) timestamp/datetime: 2033-09-04 14:59:59
                      4) date: 2015-09-01
                      */
                        Long vl;
                        if (field == null) {
                            vl = null;
                        }
                        else if (ddlType.equals("long")) {
                            Double d = Double.parseDouble(field);
                            vl = d.longValue();
                        }
                        else if (ddlType.equals("timestamp")) {
                            vl = dateTimeFormatter.parse(field).getTime();
                        }
                        else if (ddlType.equals("date")) {
                            vl = dateFormatter.parse(field).getTime();
                        }
                        else {
                            throw new RuntimeException(String.format("Unknown ddl type: %s", ddlType));
                        }
                        record.put(i, vl);
                        break;
                    case STRING:
                        record.put(i, field);
                        break;
                    default:
                        break;
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            output.write(tableName.replace('_', '4'), null, record, tableName);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
        if (output != null) {
            output.close();
            output = null;
        }
    }

    private List<String> split(String line)
    {
        List<String> fields = new ArrayList<String>();
        int prev = -1;
        boolean enclosed = true;
        for (int i = 0; i < line.length(); i++) {
            switch (line.charAt(i)) {
            case '\\':
                i += 1;
                break;
            case ',':
                if (enclosed) {
                    String field = line.substring(prev + 1, i);
                    if (field.startsWith("\"") && field.endsWith("\"")) {
                        field = field.substring(1, field.length() - 1);
                    }

                    prev = i;
                    fields.add(field);
                }
                break;
            case '"':
                enclosed = !enclosed;
                break;
            default:
                break;
            }
        }
        String field = line.substring(prev + 1);
        if (field.startsWith("\"") && field.endsWith("\"")) {
            field = field.substring(1, field.length() - 1);
        }

        fields.add(field);
        return fields;
    }
}
