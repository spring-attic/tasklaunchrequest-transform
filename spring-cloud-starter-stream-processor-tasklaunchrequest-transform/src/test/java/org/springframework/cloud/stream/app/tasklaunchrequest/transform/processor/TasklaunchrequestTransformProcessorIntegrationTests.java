/*
 * Copyright 2015-2017 the original author or authors.
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.springframework.cloud.stream.test.matcher.MessageQueueMatcher.receivesPayloadThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.cloud.task.launcher.TaskLaunchRequest;
import org.springframework.integration.transformer.MessageTransformationException;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Tests for TasklaunchrequestTransformIntegrationProcessor.
 *
 * @author Glenn Renfro
 * @author Artem Bilan
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

	/**
	 * Validates that the app loads with default properties.
	 */
	@TestPropertySource(properties = "uri=" + DEFAULT_URI)
	public static class UsingDefaultIntegrationTests extends
			TasklaunchrequestTransformProcessorIntegrationTests {

		@Test
		public void test() throws InterruptedException {
			channels.input().send(new GenericMessage<Object>("hello"));
			channels.input().send(new GenericMessage<Object>("hello world"));
			channels.input().send(new GenericMessage<Object>("hi!"));
			assertThat(collector.forChannel(channels.output()), receivesPayloadThat(is(getDefaultRequest())));
			assertThat(collector.forChannel(channels.output()), receivesPayloadThat(is(getDefaultRequest())));
			assertThat(collector.forChannel(channels.output()), receivesPayloadThat(is(getDefaultRequest())));
		}
	}

	/**
	 * Validates that the app fails without URI.
	 */
	public static class UsingNoURIIntegrationTests extends
			TasklaunchrequestTransformProcessorIntegrationTests {

		@Test(expected = MessageTransformationException.class)
		public void test() throws InterruptedException {
			channels.input().send(new GenericMessage<Object>("hello"));
		}
	}

	/**
	 * Validates that the app handles empty payload.
	 */
	@TestPropertySource(properties = "uri=" + DEFAULT_URI)
	public static class UsingEmptyPayloadIntegrationTests extends
			TasklaunchrequestTransformProcessorIntegrationTests {

		@Test()
		public void test() throws InterruptedException {
			channels.input().send(new GenericMessage<Object>(""));
			assertThat(collector.forChannel(channels.output()),
					receivesPayloadThat(is(getDefaultRequest())));
		}
	}

	/**
	 * Verify datasource properties are added to the TaskLaunchRequest.
	 */
	@TestPropertySource(properties = { "dataSourceUrl=myUrl",
			"dataSourcePassword=myPassword",
			"dataSourceUserName=myUserName",
			"dataSourceDriverClassName=myClassName",
			"uri=" + DEFAULT_URI,
	        "applicationName=fooTest"})
	public static class UsingDataSourceIntegrationTests
			extends TasklaunchrequestTransformProcessorIntegrationTests {

		@Test
		public void testDataSources() throws InterruptedException {
			channels.input().send(new GenericMessage<Object>("hello"));
			Map<String, String> environmentVariables = new HashMap<>(4);
			environmentVariables.put("spring_datasource_url", "myUrl");
			environmentVariables.put("spring_datasource_driverClassName", "myClassName");
			environmentVariables.put("spring_datasource_username", "myUserName");
			environmentVariables.put("spring_datasource_password", "myPassword");
			assertThat(collector.forChannel(channels.output()),
					receivesPayloadThat(is(getDefaultRequest(
							environmentVariables, null, null))));
		}

		@Test
		public void testForApplicationName() throws InterruptedException {
			channels.input().send(new GenericMessage<Object>("hello"));
			TaskLaunchRequest result = (TaskLaunchRequest) collector.forChannel(channels.output()).take().getPayload();
			assertThat(result.getApplicationName(), is(equalTo("fooTest")));
		}
	}

	/**
	 * Verify deploymentProperties are added to the TaskLaunchRequest.
	 */
	@TestPropertySource(properties = {
			"deploymentProperties=app.wow.hello=world,app.wow.foo=bar,app.wow.test=a=b,c=d,e=\"baz=bbb,nnn=mmm\"",
			"uri=" + DEFAULT_URI })
	public static class UsingDeploymentPropertiesIntegrationTests
			extends TasklaunchrequestTransformProcessorIntegrationTests {

		@Test
		public void test() throws InterruptedException {
			channels.input().send(new GenericMessage<Object>("hello"));
			Map<String, String> environmentVariables = new HashMap<>(3);
			Map<String, String> deploymentProperties = new HashMap<>(2);
			deploymentProperties.put("app.wow.hello", "world");
			deploymentProperties.put("app.wow.foo", "bar");
			deploymentProperties.put("app.wow.test", "a=b");
			deploymentProperties.put("c", "d");
			deploymentProperties.put("e", "\"baz=bbb,nnn=mmm\"");

			assertThat(collector.forChannel(channels.output()),
					receivesPayloadThat(is(getDefaultRequest(
							environmentVariables, deploymentProperties, null))));
		}
	}

	/**
	 *  Verify commandLineArguments are added to the TaskLaunchRequest.
	 */
	@TestPropertySource(properties = {
			"commandLineArguments=--hello=world --foo=bar",
			"uri=" + DEFAULT_URI })
	public static class UsingCommandLineArgsIntegrationTests
			extends TasklaunchrequestTransformProcessorIntegrationTests {

		@Test
		public void test() throws InterruptedException {
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

	protected TaskLaunchRequest getDefaultRequest() {
		Map<String, String> environmentVariables = new HashMap<>(1);
		return getDefaultRequest(environmentVariables, null, null);
	}

	protected TaskLaunchRequest getDefaultRequest(
			Map<String, String> environmentVariables,
			Map<String, String> deploymentProperties,
			List<String> commandLineArgs) {
		TaskLaunchRequest request = new TaskLaunchRequest(
				DEFAULT_URI,
				commandLineArgs,
				environmentVariables,
				deploymentProperties,
				"test");
		return request;
	}

	@SpringBootApplication
	public static class TasklaunchrequestTransformProcessorApplication {

	}

}
