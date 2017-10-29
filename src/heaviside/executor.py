'''
Created on Oct 29, 2017

@author: bkehoe
'''

from __future__ import absolute_import

import uuid
import itertools
import json

from . import components, states

def is_heaviside_execution(context):
    return Executor.CONTEXT_EXECUTION_ID_KEY in context

class Executor(object):
    @classmethod
    def create(cls, definition,
               definition_store,
               execution_store,
               logger_factory,
               task_dispatcher):
        execution_id = uuid.uuid4().hex
        
        if not isinstance(definition, states.StateMachine):
            definition = states.StateMachine.from_json(definition)
        
        execution = execution_store.execution_factory(execution_id, definition_store)
        execution.initialize(definition)
        
        return cls(
            execution_id,
            execution,
            definition_store,
            execution_store,
            logger_factory,
            task_dispatcher)
    
    @classmethod
    def hydrate(cls, context,
               definition_store,
               execution_store,
               logger_factory,
               task_dispatcher):
        
        execution_id = context[cls.CONTEXT_EXECUTION_ID_KEY]
        
        definition_store.hydrate(context)
        
        execution = execution_store.execution_factory(execution_id, definition_store)
        execution.hydrate(context)
        
        return cls(
            execution_id,
            execution,
            definition_store,
            execution_store,
            logger_factory,
            task_dispatcher)
    
    def __init__(self,
                 execution_id,
                 execution,
                 definition_store,
                 execution_store,
                 logger_factory,
                 task_dispatcher):
        self.execution_id = execution_id
        self.execution = execution
        self.definition = execution.get_definition()
        
        self.executor_id = uuid.uuid4().hex
        print 'created executor {}'.format(self.executor_id[-4:])
        
        self.execution = execution
        
        self.definition_store=definition_store
        self.execution_store=execution_store
        self.logger=logger_factory.logger_factory(self.execution_id, self.executor_id)
        self.task_dispatcher=task_dispatcher
    
    CONTEXT_EXECUTION_ID_KEY = 'x-heaviside-sm-eid'
    
    def get_context(self):
        context = {
            self.CONTEXT_EXECUTION_ID_KEY: self.execution_id,
        }
        context.update(self.definition_store.get_context())
        context.update(self.execution.get_context())
        context.update(self.logger.get_context())
        return context
    
    def log_state(self):
        state, result = self.execution.get_current_state_and_result()
        s = []
        s.append('\nvvv               vvv')
#         s.append('execution: {}'.format(self.execution_id[-4:]))
        s.append('executor: {}'.format(self.executor_id[-4:]))
        
        if state:
            s.append('state:' + json.dumps(state.to_json(), indent=2))
        
        if result:
            s.extend(['result:', json.dumps(result.to_json())])
        
        s.append('^^^               ^^^\n')
        print '\n'.join(s)
    
    def dispatch(self, input):
        """Run the state machine up to the next Task state, which will be async invoked."""
        print '[dispatch] {} input: {}'.format(self.executor_id[-4:], input)
        current_state, result = self.execution.get_current_state_and_result()
        self.log_state()
        if current_state is None:
            if result:
                print 'has result'
                return result.to_json()
            else:
                print 'initializing', self.definition.start_at
                self.execution.change_state(self.definition.start_at)
                self.log_state()
        for i in itertools.count():
            current_state, result = self.execution.get_current_state_and_result()
            
            print 'loop', self.executor_id[-4:], i, current_state.name
            state_def = self.definition.states[current_state.name]
            print 'state def', json.dumps(state_def.to_json())
            if isinstance(state_def, states.SucceedState):
                print 'succeed'
                self.execution.set_result(components.Result(components.Result.STATUS_SUCCEEDED))
                self.log_state()
                break
            elif isinstance(state_def, states.FailState):
                print 'fail'
                self.execution.set_result(components.Result(components.Result.STATUS_FAILED))
                self.log_state()
                break
            elif isinstance(state_def, states.TaskState):
                print 'task'
                self.task_dispatcher.dispatch(state_def.resource, input, self.get_context())
                break
            else:
                raise TypeError("No matching type for {}".format(state_def))
    
    def run_task(self, task_function, exception_handler):
        """Process the current task and dispatch.
        Assumes the current state is a Task state."""
        print '[run task] {}'.format(self.executor_id[-4:])
        
        current_state, result = self.execution.get_current_state_and_result()
        
        self.log_state()
        
        state_def = self.definition.states[current_state.name]
        print 'state def', state_def.to_json()
        try:
            output = task_function()
        except Exception as e:
            exception = exception_handler(e)
            for catcher in state_def.catch or []:
                if catcher.matches(exception):
                    self.execution.change_state(catcher.next)
                    output = {}
                    break
            else:
#                 self.result = {
#                     "type": "Fail",
#                     "name": current_state.name,
#                     "Error": "States.TaskFailed",
#                     "Cause": "No matching catcher",
#                 }
                self.execution.set_result(components.Result(components.Result.STATUS_FAILED))
                self.log_state()
                return
        else:
            if state_def.is_end():
                print 'task is end state'
                self.execution.set_result(components.Result(components.Result.STATUS_SUCCEEDED, output))
            else:
                self.execution.change_state(state_def.next)
        self.dispatch(result)
        return result