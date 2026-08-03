package com.datastream.mvp.dag;

import com.datastream.mvp.model.ControlRegistry;
import com.datastream.mvp.repository.ControlRegistryRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;

public interface DagExecutor {
    String execute(DagDefinition dag) throws Exception;
}