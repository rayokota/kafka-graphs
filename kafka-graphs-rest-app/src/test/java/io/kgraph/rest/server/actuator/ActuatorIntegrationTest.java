package io.kgraph.rest.server.actuator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;

import io.kgraph.rest.server.KafkaGraphsApplication;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = KafkaGraphsApplication.class)
public class ActuatorIntegrationTest {

    @Autowired 
    private TestRestTemplate restTemplate;

    @Test
    public void testGetInfo() throws IOException {
        final ResponseEntity<String> responseEntity = this.restTemplate.getForEntity("/actuator/info", String.class);
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());
    }
}