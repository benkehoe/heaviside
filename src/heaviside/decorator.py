'''
Created on Oct 28, 2017

@author: bkehoe
'''

from __future__ import absolute_import

from . import executor, aws

def handler(handler_function):
    """Decorator to wrap a Lambda handler to enable execution as a state machine.
    Use like:
    @heaviside.handler
    def handler(event, context):
        #proceed as normal
    """
    def wrapper(event, context):
        if not (context.client_context
                and
                hasattr(context.client_context, 'custom')
                and
                isinstance(context.client_context.custom, dict)
                and
                executor.is_heaviside_execution(context.client_context.custom)):
            return handler_function(event, context)
        
        heaviside_context = context.client_context.custom
        
        print heaviside_context
        
        ex = executor.Executor.hydrate(heaviside_context, **aws.create_components())
        
        task_runner = lambda: handler_function(event, context)
        exception_handler = lambda e: 'States.TaskFailed'
        
        return ex.run_task(task_runner, exception_handler)

    wrapper.__name__ = handler_function.__name__
    return wrapper

