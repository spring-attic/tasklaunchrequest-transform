/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.tasklaunchrequest.transform.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.cloud.task.launcher.TaskLaunchRequest;
import org.springframework.http.MediaType;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.springframework.cloud.stream.test.matcher.MessageQueueMatcher.receivesPayloadThat;

/**
 * Tests for TasklaunchrequestTransformIntegrationProcessor.
 *
 * @author Glenn Renfro
 * @author Artem Bilan
 * @author Chris Schaefer
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
		classes =
				TasklaunchrequestTransformProcessorIntegrationTests.
						TasklaunchrequestTransformProcessorApplication.class)
@DirtiesContext
public abstract class TasklaunchrequestTransformProcessorIntegrationTests {

	public static final String DEFAULT_URI = "MY_URI";

	@Autowired
	protected Processor channels;

	@Autowired
	protected MessageCollector collector;

	protected ObjectMapper objectMapper = new ObjectMapper();

	/**
	 * Validates that the app loads with default properties.
	 */
	@TestPropertySource(properties = "task.launch.request.uri=" + DEFAULT_URI)
	public static class UsingDefaultIntegrationTests extends
			TasklaunchrequestTransformProcessorIntegrationTests {

		@Test
		public void test() throws Exception {
			channels.input().send(new GenericMessage<Object>("hello"));
			channels.input().send(new GenericMessage<Object>("hello world"));
			channels.input().send(new GenericMessage<Object>("hi!"));
			validateDefault((String) collector.forChannel(channels.output()).take().getPayload());
			validateDefault((String) collector.forChannel(channels.output()).take().getPayload());
			validateDefault((String) collector.forChannel(channels.output()).take().getPayload());
		}

		@Test
		public void testHeaderContentTypeProperlySet() throws Exception {
			Message<?> message = MessageBuilder.withPayload("hello").setHeader(MessageHeaders.CONTENT_TYPE, "text/plain").build();
			channels.input().send(message);
			Message<?> outputMessage = collector.forChannel(channels.output()).take();
			assertThat(outputMessage.getHeaders().get(MessageHeaders.CONTENT_TYPE)).isEqualTo(MediaType.APPLICATION_JSON);
		}

		private void validateDefault(String result) throws Exception{
			TaskLaunchRequest tlr = this.objectMapper.readValue(result, TaskLaunchRequest.class);
			assertThat(tlr.getApplicationName()).startsWith("Task-");
			assertThat(tlr.getUri()).isEqualTo(DEFAULT_URI);
			assertThat(tlr.getCommandlineArguments().size()).isEqualTo(0);
			assertThat(tlr.getEnvironmentProperties().size()).isEqualTo(0);
			assertThat(tlr.getDeploymentProperties().size()).isEqualTo(0);
		}
	}

	/**
	 * Validates that the app fails without URI.
	 */
	public static class UsingNoURIIntegrationTests extends
			TasklaunchrequestTransformProcessorIntegrationTests {

		@Test(expected = MessagingException.class)
		public void test() throws InterruptedException {
			channels.input().send(new GenericMessage<Object>("hello"));
		}
	}


	/**
	 * Validates that the app handles empty payload.
	 */
	@TestPropertySource(properties = {"task.launch.request.uri=" + DEFAULT_URI, "task.launch.request.applicationName=test"})
	public static class UsingEmptyPayloadIntegrationTests extends
			TasklaunchrequestTransformProcessorIntegrationTests {

		@Test()
		public void test() throws Exception {
			channels.input().send(new GenericMessage<Object>(""));
			assertThat(collector.forChannel(channels.output()),
					receivesPayloadThat(is(getDefaultRequest())));
		}
	}

	/**
	 * Verify datasource properties are added to the TaskLaunchRequest.
	 */
	@TestPropertySource(properties = { "task.launch.request.dataSourceUrl=myUrl",
			"task.launch.request.dataSourcePassword=myPassword",
			"task.launch.request.dataSourceUserName=myUserName",
			"task.launch.request.dataSourceDriverClassName=myClassName",
			"task.launch.request.uri=" + DEFAULT_URI,
	        "task.launch.request.applicationName=test"})
	public static class UsingDataSourceIntegrationTests
			extends TasklaunchrequestTransformProcessorIntegrationTests {

		@Test
		public void testDataSources() throws Exception {
			channels.input().send(new GenericMessage<Object>("hello"));
			Map<String, String> environmentVariables = new HashMap<>(4);
			environmentVariables.put("spring.datasource.url", "myUrl");
			environmentVariables.put("spring.datasource.driver-class-name", "myClassName");
			environmentVariables.put("spring.datasource.username", "myUserName");
			environmentVariables.put("spring.datasource.password", "myPassword");
			assertThat(collector.forChannel(channels.output()),
					receivesPayloadThat(is(getDefaultRequest(
							environmentVariables, null, null))));
		}

		@Test
		public void testForApplicationName() throws Exception {
			channels.input().send(new GenericMessage<Object>("hello"));
			TaskLaunchRequest result = objectMapper.readValue(
					(String) collector.forChannel(channels.output()).take().
							getPayload(), TaskLaunchRequest.class);
			assertThat(result.getApplicationName()).isEqualTo("test");
		}
	}

	/**
	 * Verify deploymentProperties are added to the TaskLaunchRequest.
	 */
	@TestPropertySource(properties = {
			"task.launch.request.deploymentProperties=app.wow.hello=world,app.wow.foo=bar,app.wow.test=a=b,c=d,e=\"baz=bbb,nnn=mmm\"",
			"task.launch.request.environmentProperties=app.wow.hello2=world2,app.wow.foo2=bar2,app.wow.test2=a=b2,c2=d2,e2=\"baz=bbb,nnn=mmm2\"",
			"task.launch.request.uri=" + DEFAULT_URI,
			"task.launch.request.applicationName=test"})
	public static class UsingDeploymentPropertiesIntegrationTests
			extends TasklaunchrequestTransformProcessorIntegrationTests {

		@Test
		public void test() throws Exception {
			channels.input().send(new GenericMessage<Object>("hello"));
			String result = (String) collector.forChannel(channels.output()).take().getPayload();
			TaskLaunchRequest tlr = this.objectMapper.readValue(result, TaskLaunchRequest.class);
			assertThat(tlr.getDeploymentProperties().get("c")).isEqualTo("d");
			assertThat(tlr.getDeploymentProperties().get("app.wow.hello")).isEqualTo("world");
			assertThat(tlr.getDeploymentProperties().get("app.wow.foo")).isEqualTo("bar");
			assertThat(tlr.getDeploymentProperties().get("app.wow.test")).isEqualTo("a=b");
			assertThat(tlr.getDeploymentProperties().get("e")).isEqualTo("\"baz=bbb,nnn=mmm\"");
			assertThat(tlr.getEnvironmentProperties().get("c2")).isEqualTo("d2");
			assertThat(tlr.getEnvironmentProperties().get("app.wow.hello2")).isEqualTo("world2");
			assertThat(tlr.getEnvironmentProperties().get("app.wow.foo2")).isEqualTo("bar2");
			assertThat(tlr.getEnvironmentProperties().get("app.wow.test2")).isEqualTo("a=b2");
			assertThat(tlr.getEnvironmentProperties().get("e2")).isEqualTo("\"baz=bbb,nnn=mmm2\"");
			assertThat(tlr.getApplicationName()).isEqualTo("test");
			assertThat(tlr.getUri()).isEqualTo(DEFAULT_URI);
			assertThat(tlr.getDeploymentProperties().size()).isEqualTo(5);
			assertThat(tlr.getEnvironmentProperties().size()).isEqualTo(5);
			assertThat(tlr.getCommandlineArguments().size()).isEqualTo(0);
		}
	}

	/**
	 *  Verify commandLineArguments are added to the TaskLaunchRequest.
	 */
	@TestPropertySource(properties = {
			"task.launch.request.commandLineArguments=--hello=world --foo=bar",
			"task.launch.request.uri=" + DEFAULT_URI,
			"task.launch.request.applicationName=test" })
	public static class UsingCommandLineArgsIntegrationTests
			extends TasklaunchrequestTransformProcessorIntegrationTests {

		@Test
		public void test() throws Exception {
			channels.input().send(new GenericMessage<Object>("hello"));
			Map<String, String> environmentVariables = new HashMap<>(1);
			List<String> commandLineArgs = new ArrayList<>(2);
			commandLineArgs.add("--hello=world");
			commandLineArgs.add("--foo=bar");
			assertThat(collector.forChannel(channels.output()),
					receivesPayloadThat(is(getDefaultRequest(
							environmentVariables, null, commandLineArgs))));
		}
	}

	protected String getDefaultRequest() throws Exception{
		Map<String, String> environmentVariables = new HashMap<>(1);
		return getDefaultRequest(environmentVariables, null, null);
	}

	protected String getDefaultRequest(
			Map<String, String> environmentVariables,
			Map<String, String> deploymentProperties,
			List<String> commandLineArgs) throws Exception{
		TaskLaunchRequest request = new TaskLaunchRequest(
				DEFAULT_URI,
				commandLineArgs,
				environmentVariables,
				deploymentProperties,
				"test");
		return objectMapper.writeValueAsString(request);
	}

	@SpringBootApplication
	public static class TasklaunchrequestTransformProcessorApplication {

	}

}
