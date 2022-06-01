/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg;

import com.google.common.cache.CacheBuilder;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.jmx.CacheStatsMBean;
import io.trino.collect.cache.NonEvictableCache;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.cache.CachingParquetMetadataSource;
import io.trino.parquet.cache.ParquetCacheConfig;
import io.trino.parquet.cache.ParquetFileMetadata;
import io.trino.parquet.cache.ParquetMetadataSource;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.procedure.ExpireSnapshotsTableProcedure;
import io.trino.plugin.iceberg.procedure.OptimizeTableProcedure;
import io.trino.plugin.iceberg.procedure.RemoveOrphanFilesTableProcedure;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.procedure.Procedure;
import org.weakref.jmx.MBeanExporter;

import javax.inject.Singleton;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class IcebergModule
        implements Module
{
    private final String connectorId;

    public IcebergModule(String connectorId)
    {
        this.connectorId = connectorId;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(IcebergTransactionManager.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(HiveConfig.class);
        configBinder(binder).bindConfig(IcebergConfig.class);
        configBinder(binder).bindConfig(MetastoreConfig.class);
        configBinder(binder).bindConfig(ParquetCacheConfig.class, connectorId);
        newSetBinder(binder, SessionPropertiesProvider.class).addBinding().to(IcebergSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(IcebergTableProperties.class).in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class).to(IcebergSplitManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorPageSourceProvider.class).setDefault().to(IcebergPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSinkProvider.class).to(IcebergPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(IcebergNodePartitioningProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(OrcReaderConfig.class);
        configBinder(binder).bindConfig(OrcWriterConfig.class);

        configBinder(binder).bindConfig(ParquetReaderConfig.class);
        configBinder(binder).bindConfig(ParquetWriterConfig.class);

        binder.bind(IcebergMetadataFactory.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(CommitTaskData.class);

        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        binder.bind(IcebergFileWriterFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(IcebergFileWriterFactory.class).withGeneratedName();

        Multibinder<Procedure> procedures = newSetBinder(binder, Procedure.class);
        procedures.addBinding().toProvider(RollbackToSnapshotProcedure.class).in(Scopes.SINGLETON);

        Multibinder<TableProcedureMetadata> tableProcedures = newSetBinder(binder, TableProcedureMetadata.class);
        tableProcedures.addBinding().toProvider(OptimizeTableProcedure.class).in(Scopes.SINGLETON);
        tableProcedures.addBinding().toProvider(ExpireSnapshotsTableProcedure.class).in(Scopes.SINGLETON);
        tableProcedures.addBinding().toProvider(RemoveOrphanFilesTableProcedure.class).in(Scopes.SINGLETON);
    }

    @Singleton
    @Provides
    public ParquetMetadataSource createParquetMetadataSource(ParquetCacheConfig parquetCacheConfig, MBeanExporter exporter)
    {
        ParquetMetadataSource parquetMetadataSource = new MetadataReader();
        if (parquetCacheConfig.isMetadataCacheEnabled()) {
            NonEvictableCache<ParquetDataSourceId, ParquetFileMetadata> cache = buildNonEvictableCache(CacheBuilder.newBuilder().maximumWeight(parquetCacheConfig.getMetadataCacheSize().toBytes())
                    .weigher((id, metadata) -> ((ParquetFileMetadata) metadata).getMetadataSize())
                    .expireAfterAccess(parquetCacheConfig.getMetadataCacheTtlSinceLastAccess().toMillis(), MILLISECONDS)
                    .recordStats());
            CacheStatsMBean cacheStatsMBean = new CacheStatsMBean(cache);
            parquetMetadataSource = new CachingParquetMetadataSource(cache, parquetMetadataSource);
            exporter.export(generatedNameOf(CacheStatsMBean.class, connectorId + "_ParquetMetadata"), cacheStatsMBean);
        }
        return parquetMetadataSource;
    }
}
