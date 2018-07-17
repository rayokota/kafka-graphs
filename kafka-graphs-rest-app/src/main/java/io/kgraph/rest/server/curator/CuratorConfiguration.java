package io.kgraph.rest.server.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.kgraph.rest.server.KafkaGraphsProperties;

@Configuration
@EnableConfigurationProperties(KafkaGraphsProperties.class)
public class CuratorConfiguration {  //implements BeanFactoryAware {

    private final KafkaGraphsProperties props;

    //private BeanFactory beanFactory;

    public CuratorConfiguration(KafkaGraphsProperties props) {
        this.props = props;
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    public CuratorFramework curatorFramework(RetryPolicy retryPolicy) {

        final CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        builder
            .connectString(props.getZookeeperConnect())
            .retryPolicy(retryPolicy);
        return builder.build();
    }

    @Bean
    public RetryPolicy retryPolicy() {
        return new ExponentialBackoffRetry(1000, 3);
        //return new ExponentialBackoffRetry(props.getSleepTimeMs(), props.getMaxRetries());
    }

    /*
    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }
    */
}
