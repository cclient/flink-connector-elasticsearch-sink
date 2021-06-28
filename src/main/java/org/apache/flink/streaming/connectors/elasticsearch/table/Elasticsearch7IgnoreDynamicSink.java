//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.flink.streaming.connectors.elasticsearch.table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink.Builder;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;

@Internal
class Elasticsearch7IgnoreDynamicSink implements DynamicTableSink {
    @VisibleForTesting
    static final Elasticsearch7IgnoreDynamicSink.Elasticsearch7RequestFactory REQUEST_FACTORY = new Elasticsearch7IgnoreDynamicSink.Elasticsearch7RequestFactory();
    private final EncodingFormat<SerializationSchema<RowData>> format;
    private final TableSchema schema;
    private final TableSchema schemaWithIgnoreFields;
    private final Elasticsearch7Configuration config;
    private final Elasticsearch7IgnoreDynamicSink.ElasticSearchBuilderProvider builderProvider;

    private final List<String> ignoreFields;
    private final List<Integer> ignoreFieldIndeies;

    public Elasticsearch7IgnoreDynamicSink(EncodingFormat<SerializationSchema<RowData>> format, Elasticsearch7Configuration config, TableSchema schema,List<String> ignoreFields) {
        this(format, config, schema, Builder::new,ignoreFields);
    }

    Elasticsearch7IgnoreDynamicSink(EncodingFormat<SerializationSchema<RowData>> format, Elasticsearch7Configuration config, TableSchema schema, Elasticsearch7IgnoreDynamicSink.ElasticSearchBuilderProvider builderProvider,List<String> ignoreFields) {
        this.format = format;
        this.schema = schema;
        this.ignoreFields=ignoreFields;
        this.ignoreFieldIndeies=new ArrayList<>();
        List<TableColumn> orginalTableColumns=schema.getTableColumns();
        List<TableColumn> destTableColumns=new ArrayList<>(orginalTableColumns.size());
        for (int i = 0; i < orginalTableColumns.size(); i++) {
            TableColumn tableColumn=orginalTableColumns.get(i);
            if(ignoreFields.contains(tableColumn.getName())){
                ignoreFieldIndeies.add(i);
            }else{
                destTableColumns.add(tableColumn);
            }
        }
        String[] fieldNames=(String[])destTableColumns.stream().map(TableColumn::getName).toArray((x$0) -> {
            return new String[x$0];
        });
        DataType[] fieldDataTypes=(DataType[])destTableColumns.stream().map(TableColumn::getType).toArray((x$0) -> { return new DataType[x$0]; });
        TypeInformation<?>[] typeInformations=TypeConversions.fromDataTypeToLegacyInfo(fieldDataTypes);
        this.schemaWithIgnoreFields=new TableSchema(fieldNames,typeInformations);
        this.config = config;
        this.builderProvider = builderProvider;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        org.apache.flink.table.connector.ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        Iterator var3 = requestedMode.getContainedKinds().iterator();

        while(var3.hasNext()) {
            RowKind kind = (RowKind)var3.next();
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }

        return builder.build();
    }

    @Override
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
        return () -> {
            SerializationSchema<RowData> format = (SerializationSchema)this.format.createRuntimeEncoder(context, this.schemaWithIgnoreFields.toRowDataType());
            RowElasticsearchSinkFunction upsertFunction = new RowElasticsearchIgnoreSinkFunction(IndexGeneratorFactory.createIndexGenerator(this.config.getIndex(), this.schema), (String)null, format, XContentType.JSON, REQUEST_FACTORY, KeyExtractor.createKeyExtractor(this.schema, this.config.getKeyDelimiter()),ignoreFieldIndeies);
            Builder<RowData> builder = this.builderProvider.createBuilder(this.config.getHosts(), upsertFunction);
            builder.setFailureHandler(this.config.getFailureHandler());
            builder.setBulkFlushMaxActions(this.config.getBulkFlushMaxActions());
            builder.setBulkFlushMaxSizeMb((int)(this.config.getBulkFlushMaxByteSize() >> 20));
            builder.setBulkFlushInterval(this.config.getBulkFlushInterval());
            builder.setBulkFlushBackoff(this.config.isBulkFlushBackoffEnabled());
            this.config.getBulkFlushBackoffType().ifPresent(builder::setBulkFlushBackoffType);
            this.config.getBulkFlushBackoffRetries().ifPresent(builder::setBulkFlushBackoffRetries);
            this.config.getBulkFlushBackoffDelay().ifPresent(builder::setBulkFlushBackoffDelay);
            if (this.config.getUsername().isPresent() && this.config.getPassword().isPresent() && !StringUtils.isNullOrWhitespaceOnly((String)this.config.getUsername().get()) && !StringUtils.isNullOrWhitespaceOnly((String)this.config.getPassword().get())) {
                builder.setRestClientFactory(new Elasticsearch7IgnoreDynamicSink.AuthRestClientFactory((String)this.config.getPathPrefix().orElse((String) null), (String)this.config.getUsername().get(), (String)this.config.getPassword().get()));
            } else {
                builder.setRestClientFactory(new Elasticsearch7IgnoreDynamicSink.DefaultRestClientFactory((String)this.config.getPathPrefix().orElse((String) null)));
            }

            ElasticsearchSink<RowData> sink = builder.build();
            if (this.config.isDisableFlushOnCheckpoint()) {
                sink.disableFlushOnCheckpoint();
            }

            return sink;
        };
    }

    @Override
    public DynamicTableSink copy() {
        return this;
    }

    @Override
    public String asSummaryString() {
        return "Elasticsearch7";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            Elasticsearch7IgnoreDynamicSink that = (Elasticsearch7IgnoreDynamicSink)o;
            return Objects.equals(this.format, that.format) && Objects.equals(this.schema, that.schema) && Objects.equals(this.config, that.config) && Objects.equals(this.builderProvider, that.builderProvider);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(new Object[]{this.format, this.schema, this.config, this.builderProvider});
    }

    private static class Elasticsearch7RequestFactory implements RequestFactory {
        private Elasticsearch7RequestFactory() {
        }

        @Override
        public UpdateRequest createUpdateRequest(String index, String docType, String key, XContentType contentType, byte[] document) {
            return (new UpdateRequest(index, key)).doc(document, contentType).upsert(document, contentType);
        }

        @Override
        public IndexRequest createIndexRequest(String index, String docType, String key, XContentType contentType, byte[] document) {
            return (new IndexRequest(index)).id(key).source(document, contentType);
        }

        @Override
        public DeleteRequest createDeleteRequest(String index, String docType, String key) {
            return new DeleteRequest(index, key);
        }
    }

    @VisibleForTesting
    static class AuthRestClientFactory implements RestClientFactory {
        private final String pathPrefix;
        private final String username;
        private final String password;
        private transient CredentialsProvider credentialsProvider;

        public AuthRestClientFactory(@Nullable String pathPrefix, String username, String password) {
            this.pathPrefix = pathPrefix;
            this.password = password;
            this.username = username;
        }

        @Override
        public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
            if (this.pathPrefix != null) {
                restClientBuilder.setPathPrefix(this.pathPrefix);
            }

            if (this.credentialsProvider == null) {
                this.credentialsProvider = new BasicCredentialsProvider();
                this.credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(this.username, this.password));
            }

            restClientBuilder.setHttpClientConfigCallback((httpAsyncClientBuilder) -> {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(this.credentialsProvider);
            });
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o != null && this.getClass() == o.getClass()) {
                Elasticsearch7IgnoreDynamicSink.AuthRestClientFactory that = (Elasticsearch7IgnoreDynamicSink.AuthRestClientFactory)o;
                return Objects.equals(this.pathPrefix, that.pathPrefix) && Objects.equals(this.username, that.username) && Objects.equals(this.password, that.password);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(new Object[]{this.pathPrefix, this.password, this.username});
        }
    }

    @VisibleForTesting
    static class DefaultRestClientFactory implements RestClientFactory {
        private final String pathPrefix;

        public DefaultRestClientFactory(@Nullable String pathPrefix) {
            this.pathPrefix = pathPrefix;
        }

        @Override
        public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
            if (this.pathPrefix != null) {
                restClientBuilder.setPathPrefix(this.pathPrefix);
            }

        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o != null && this.getClass() == o.getClass()) {
                Elasticsearch7IgnoreDynamicSink.DefaultRestClientFactory that = (Elasticsearch7IgnoreDynamicSink.DefaultRestClientFactory)o;
                return Objects.equals(this.pathPrefix, that.pathPrefix);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(new Object[]{this.pathPrefix});
        }
    }

    @FunctionalInterface
    interface ElasticSearchBuilderProvider {
        Builder<RowData> createBuilder(List<HttpHost> var1, RowElasticsearchSinkFunction var2);
    }
}
