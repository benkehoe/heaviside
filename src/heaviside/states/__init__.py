"""
Classes to represent state machine definitions in the states language. https://states-language.net/spec.html
"""

import json

class StateMachine(object):
    @classmethod
    def from_json(cls, obj):
        if isinstance(obj, basestring):
            obj = json.loads(obj)
        return cls(
            states = obj["States"],
            start_at = obj["StartAt"],
            comment = obj.get("Comment"),
            version = obj.get("Version"),
            timeout_seconds = obj.get("TimeoutSeconds"),
        )
        
    
    def __init__(self, states, start_at, comment=None, version=None, timeout_seconds=None):
        self.states = states
        self.start_at = start_at
        self.comment = comment
        self.version = version or "1.0"
        self.timeout_seconds = timeout_seconds
    
    def to_json(self):
        data = {
            "States": self.states,
            "StartAt": self.start_at,
            "Version": self.version,
        }
        if self.comment is not None:
            data["Comment"] = self.comment
        if self.timeout_seconds is not None:
            data["TimeoutSeconds"] = self.timeout_seconds
        return data

class State(object):
    @classmethod
    def from_json(cls, obj):
        raise NotImplementedError
    
    def __init__(self, type, comment=None):
        self.type = type
        self.comment = comment
    
    def is_end(self):
        raise NotImplementedError
    
    def to_json(self):
        data = {
            "Type": self.type,
        }
        if self.comment is not None:
            data["Comment"] = self.comment
        return data

class Catcher(object):
    @classmethod
    def TaskFailed(cls, next):
        return cls(["States.TaskFailed"], next)
    
    @classmethod
    def ALL(cls, next):
        return cls(["States.ALL"], next)
    
    def __init__(self, error_equals, next):
        self.error_equals = error_equals
        self.next = next
    
    def matches(self, error):
        return error in self.error_equals or 'States.ALL' in self.error_equals

class TaskState(State):
    @classmethod
    def from_json(cls, obj):
        if obj["Type"] != "Task":
            raise TypeError("Data is not a Task state")
        return cls(
            obj.get("Next"),
            catch = obj.get("Catch"),
            comment = obj.get("Comment"))
    
    def __init__(self, resource, next, catch=None, comment=None):
        super(TaskState, self).__init__("Task", comment=comment)
        self.resource = resource
        self.next = next
        self.catch = catch
    
    def is_end(self):
        return next is None
    
    def to_json(self):
        data = super(TaskState, self).to_json()
        data["Resource"] = self.resource
        if self.next is None:
            data["End"] = True
        else:
            data["Next"] = self.next
        if self.catch is not None:
            data["Catch"] = self.catch
        return data

class SuccessState(State):
    @classmethod
    def from_json(cls, obj):
        if obj["Type"] != "Success":
            raise TypeError("Data is not a Success state")
        return cls(
            comment = obj.get("Comment"))
    
    def __init__(self, comment=None):
        super(SuccessState, self).__init__("Success", comment=comment)
    
    def is_end(self):
        return True
    
    def to_json(self):
        data = super(SuccessState, self).to_json()
        return data

class FailState(State):
    @classmethod
    def from_json(cls, obj):
        if obj["Type"] != "Fail":
            raise TypeError("Data is not a Fail state")
        return cls(
            obj["Error"],
            obj["Cause"],
            comment = obj.get("Comment"))
    
    def __init__(self, error, cause, comment=None):
        super(FailState, self).__init__("Fail", comment=comment)
        self.error = error
        self.cause = cause
    
    def is_end(self):
        return True
    
    def to_json(self):
        data = super(FailState, self).to_json()
        data["Error"] = self.error
        data["Cause"] = self.cause
        return data