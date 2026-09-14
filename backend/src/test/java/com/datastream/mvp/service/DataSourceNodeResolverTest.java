package com.datastream.mvp.service;

import com.datastream.mvp.dag.DagDefinition;
import com.datastream.mvp.model.DataSourceConnection;
import com.datastream.mvp.model.DataSourceType;
import com.datastream.mvp.repository.DataSourceConnectionRepository;
import com.datastream.mvp.security.CurrentUser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class DataSourceNodeResolverTest {
    private DataSourceConnectionRepository repository;
    private DataSourceSecretCipher cipher;
    private DataSourceNodeResolver resolver;

    @BeforeEach
    void setUp() {
        repository = mock(DataSourceConnectionRepository.class);
        cipher = mock(DataSourceSecretCipher.class);
        resolver = new DataSourceNodeResolver(repository, cipher, new ObjectMapper());
    }

    private DataSourceConnection connection(Long id, Long ownerId, DataSourceType type, String config) {
        DataSourceConnection c = new DataSourceConnection();
        c.setId(id);
        c.setName("ds-" + id);
        c.setType(type);
        c.setOwnerId(ownerId);
        c.setEnabled(true);
        c.setConfigJson(config);
        return c;
    }

    private DagDefinition dagWith(String nodeType, Object dataSourceId) {
        DagDefinition dag = new DagDefinition();
        DagDefinition.DagNode node = new DagDefinition.DagNode();
        node.setId("n1");
        node.setType(nodeType);
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("dataSourceId", dataSourceId);
        params.put("table", "orders");
        node.setParams(params);
        dag.setNodes(new ArrayList<>(List.of(node)));
        return dag;
    }

    @Test
    void leavesLegacyDagWithoutReferenceUntouched() {
        DagDefinition dag = new DagDefinition();
        DagDefinition.DagNode node = new DagDefinition.DagNode();
        node.setId("n1");
        node.setType("csv_input");
        node.setParams(new LinkedHashMap<>(Map.of("path", "/data/x.csv")));
        dag.setNodes(new ArrayList<>(List.of(node)));

        resolver.resolve(dag, new CurrentUser(7L, "u", "U", "OPERATOR"));

        assertEquals("/data/x.csv", dag.getNodes().get(0).getParams().get("path"));
        assertFalse(dag.getNodes().get(0).getParams().containsKey("dataSourceId"));
        verifyNoInteractions(repository);
    }

    @Test
    void mergesMysqlConnectionParamsAndKeepsAssetLevelParams() {
        when(cipher.decrypt(any())).thenReturn("{\"username\":\"root\",\"password\":\"secret\"}");
        DataSourceConnection withCred = connection(3L, 7L, DataSourceType.MYSQL,
                "{\"host\":\"localhost\",\"port\":3306,\"database\":\"flink_demo\"}");
        withCred.setCredentialConfigured(true);
        when(repository.findById(3L)).thenReturn(Optional.of(withCred));

        DagDefinition dag = dagWith("mysql_input", 3);
        resolver.resolve(dag, new CurrentUser(7L, "u", "U", "OPERATOR"));

        Map<String, Object> params = dag.getNodes().get(0).getParams();
        assertTrue(String.valueOf(params.get("url")).startsWith("jdbc:mysql://localhost:3306/flink_demo"));
        assertEquals("orders", params.get("table"), "资产级参数必须保留");
        assertEquals("root", params.get("username"));
        assertEquals("secret", params.get("password"));
    }

    @Test
    void missingDataSourceFailsFast() {
        when(repository.findById(99L)).thenReturn(Optional.empty());
        DagDefinition dag = dagWith("mysql_input", 99);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> resolver.resolve(dag, new CurrentUser(7L, "u", "U", "OPERATOR")));
        assertTrue(ex.getMessage().contains("数据源不存在"));
    }

    @Test
    void disabledDataSourceIsRejected() {
        DataSourceConnection disabled = connection(5L, 7L, DataSourceType.KAFKA, "{}");
        disabled.setEnabled(false);
        when(repository.findById(5L)).thenReturn(Optional.of(disabled));

        assertThrows(IllegalArgumentException.class,
                () -> resolver.resolve(dagWith("kafka_input", 5), new CurrentUser(7L, "u", "U", "OPERATOR")));
    }

    @Test
    void rejectsCrossOwnerUsageForNonAdmin() {
        when(repository.findById(8L)).thenReturn(Optional.of(connection(8L, 7L, DataSourceType.MYSQL,
                "{\"host\":\"localhost\",\"port\":3306,\"database\":\"d\"}")));

        assertThrows(IllegalArgumentException.class,
                () -> resolver.resolve(dagWith("mysql_input", 8), new CurrentUser(9L, "other", "O", "OPERATOR")));
    }

    @Test
    void ownerResolutionRejectsDataSourceOwnedBySomeoneElse() {
        when(repository.findById(8L)).thenReturn(Optional.of(connection(8L, 7L, DataSourceType.MYSQL,
                "{\"host\":\"localhost\",\"port\":3306,\"database\":\"d\"}")));

        assertThrows(IllegalArgumentException.class, () -> resolver.resolveForOwner(dagWith("mysql_input", 8), 9L));
    }

    @Test
    void validateReportsTypeMismatchWithoutSideEffects() {
        when(repository.findById(4L)).thenReturn(Optional.of(connection(4L, 7L, DataSourceType.KAFKA,
                "{\"bootstrapServers\":\"localhost:29092\"}")));
        List<String> errors = new ArrayList<>();

        resolver.validate(dagWith("mysql_input", 4), new CurrentUser(7L, "u", "U", "OPERATOR"), errors, null);

        assertEquals(1, errors.size());
        assertTrue(errors.get(0).contains("与控件"));
    }

    @Test
    void oracleUrlUsesServiceNameForm() {
        DataSourceConnection oracle = connection(6L, 7L, DataSourceType.ORACLE,
                "{\"host\":\"localhost\",\"port\":1521,\"database\":\"FREEPDB1\",\"serviceName\":\"FREEPDB1\"}");
        when(repository.findById(6L)).thenReturn(Optional.of(oracle));

        DagDefinition dag = dagWith("oracle_input", 6);
        resolver.resolve(dag, new CurrentUser(7L, "u", "U", "OPERATOR"));

        assertEquals("jdbc:oracle:thin:@//localhost:1521/FREEPDB1", dag.getNodes().get(0).getParams().get("url"));
    }
}
