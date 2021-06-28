package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class RowElasticsearchIgnoreSinkFunction extends RowElasticsearchSinkFunction implements ElasticsearchSinkFunction<RowData> {
    private static final long serialVersionUID = 1L;
    private final IndexGenerator indexGenerator;
    private final String docType;
    private final SerializationSchema<RowData> serializationSchema;
    private final XContentType contentType;
    private final RequestFactory requestFactory;
    private final Function<RowData, String> createKey;
    private final int[]  ignoreFieldIndeies;

    public RowElasticsearchIgnoreSinkFunction(IndexGenerator indexGenerator, @Nullable String docType, SerializationSchema<RowData> serializationSchema, XContentType contentType, RequestFactory requestFactory, Function<RowData, String> createKey,List<Integer> ignoreFieldIndeies) {
        super(indexGenerator,docType,serializationSchema,contentType,requestFactory,createKey);
        this.indexGenerator = (IndexGenerator) Preconditions.checkNotNull(indexGenerator);
        this.docType = docType;
        this.serializationSchema = (SerializationSchema)Preconditions.checkNotNull(serializationSchema);
        this.contentType = (XContentType)Preconditions.checkNotNull(contentType);
        this.requestFactory = (RequestFactory)Preconditions.checkNotNull(requestFactory);
        this.createKey = (Function)Preconditions.checkNotNull(createKey);
        this.ignoreFieldIndeies=new int[ignoreFieldIndeies.size()];
        for (int i = 0; i < ignoreFieldIndeies.size(); i++) {
            this.ignoreFieldIndeies[i]=ignoreFieldIndeies.get(i);
        }
    }

    @Override
    public void open() {
        this.indexGenerator.open();
    }

    @Override
    public void process(RowData element, RuntimeContext ctx, RequestIndexer indexer) {
        switch(element.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                this.processUpsert(element, indexer);
                break;
            case UPDATE_BEFORE:
            case DELETE:
                this.processDelete(element, indexer);
                break;
            default:
                throw new TableException("Unsupported message kind: " + element.getRowKind());
        }

    }

    public boolean intArrayContains(int[] ignoreFieldIndeies,int num){
        for (int k = 0; k < ignoreFieldIndeies.length; k++) {
            if(ignoreFieldIndeies[k]==num){
                return true;
            }
        }
        return false;
    }

    public RowData rowDataRemoveFiledByIndex(RowData row,int[] ignoreFieldIndeies){
        Object[] newRows=new Object[row.getArity()-ignoreFieldIndeies.length];
        for (int i = 0,j=0; i < row.getArity(); i++) {
            if(!intArrayContains(ignoreFieldIndeies,i)){
                newRows[j]=((GenericRowData)row).getField(i);
                j++;
            }
        }
        return GenericRowData.of(newRows);
    }

    private void processUpsert(RowData row, RequestIndexer indexer) {
        RowData rowData=rowDataRemoveFiledByIndex(row,this.ignoreFieldIndeies);
        byte[] document = this.serializationSchema.serialize(rowData);
        String key = (String)this.createKey.apply(row);
        if (key != null) {
            UpdateRequest updateRequest = this.requestFactory.createUpdateRequest(this.indexGenerator.generate(row), this.docType, key, this.contentType, document);
            indexer.add(new UpdateRequest[]{updateRequest});
        } else {
            IndexRequest indexRequest = this.requestFactory.createIndexRequest(this.indexGenerator.generate(row), this.docType, key, this.contentType, document);
            indexer.add(new IndexRequest[]{indexRequest});
        }

    }

    private void processDelete(RowData row, RequestIndexer indexer) {
        String key = (String)this.createKey.apply(row);
        DeleteRequest deleteRequest = this.requestFactory.createDeleteRequest(this.indexGenerator.generate(row), this.docType, key);
        indexer.add(new DeleteRequest[]{deleteRequest});
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            RowElasticsearchIgnoreSinkFunction that = (RowElasticsearchIgnoreSinkFunction)o;
            return Objects.equals(this.indexGenerator, that.indexGenerator) && Objects.equals(this.docType, that.docType) && Objects.equals(this.serializationSchema, that.serializationSchema) && this.contentType == that.contentType && Objects.equals(this.requestFactory, that.requestFactory) && Objects.equals(this.createKey, that.createKey);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(new Object[]{this.indexGenerator, this.docType, this.serializationSchema, this.contentType, this.requestFactory, this.createKey});
    }
}
