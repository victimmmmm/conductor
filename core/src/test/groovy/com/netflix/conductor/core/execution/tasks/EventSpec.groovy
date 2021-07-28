/*
 *  Copyright 2021 Netflix, Inc.
 *  <p>
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *  <p>
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  <p>
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations under the License.
 */

package com.netflix.conductor.core.execution.tasks

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.conductor.common.config.TestObjectMapperConfiguration
import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.netflix.conductor.core.events.EventQueues
import com.netflix.conductor.core.events.MockQueueProvider
import com.netflix.conductor.core.utils.ParametersUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ContextConfiguration
import spock.lang.Specification
import spock.lang.Subject

@ContextConfiguration(classes = TestObjectMapperConfiguration.class)
class EventSpec extends Specification {

    WorkflowDef testWorkflowDefinition
    EventQueues eventQueues
    ParametersUtils parametersUtils

    @Autowired
    ObjectMapper objectMapper

    @Subject
    Event event

    def setup() {
        parametersUtils = new ParametersUtils(objectMapper)
        eventQueues = new EventQueues(["sqs": new MockQueueProvider("sqs"), "conductor": new MockQueueProvider("conductor")], parametersUtils)

        testWorkflowDefinition = new WorkflowDef(name: "testWorkflow", version: 2)

        event = new Event(eventQueues, parametersUtils, objectMapper)
    }

    def "testEvent"() {
        given:
        System.setProperty("QUEUE_NAME", "queue_name_001")
        String eventt = 'queue_${QUEUE_NAME}'

        when:
        String event = parametersUtils.replace(eventt).toString()

        then:
        event && event == "queue_queue_name_001"

        when:
        eventt = "queue_9"
        event = parametersUtils.replace(eventt).toString()

        then:
        event && event == eventt
    }
}
