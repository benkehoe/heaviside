"""
Classes to represent state machine definitions in the states language. https://states-language.net/spec.html
"""

import json
import hashlib

class StateMachine(object):
    @classmethod
    def from_json(cls, obj):
        if isinstance(obj, basestring):
            obj = json.loads(obj)
        return cls(
            states = dict((key, state_from_json(value)) for key, value in obj["States"].iteritems()),
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
            "States": dict((key, state.to_json()) for key, state in self.states.iteritems()),
            "StartAt": self.start_at,
            "Version": self.version,
        }
        if self.comment is not None:
            data["Comment"] = self.comment
        if self.timeout_seconds is not None:
            data["TimeoutSeconds"] = self.timeout_seconds
        return data
    
    def get_hash(self):
        json_str = json.dumps(self.to_json())
        hasher = hashlib.sha256()
        hasher.update(json_str)
        return hasher.hexdigest()

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
            obj["Resource"],
            obj.get("Next"),
            catch = obj.get("Catch"),
            comment = obj.get("Comment"))
    
    def __init__(self, resource, next, catch=None, comment=None):
        super(TaskState, self).__init__("Task", comment=comment)
        self.resource = resource
        self.next = next
        self.catch = catch
    
    def is_end(self):
        return self.next is None
    
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

class SucceedState(State):
    @classmethod
    def from_json(cls, obj):
        if obj["Type"] != "Succeed":
            raise TypeError("Data is not a Succeed state")
        return cls(
            comment = obj.get("Comment"))
    
    def __init__(self, comment=None):
        super(SucceedState, self).__init__("Succeed", comment=comment)
    
    def is_end(self):
        return True
    
    def to_json(self):
        data = super(SucceedState, self).to_json()
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

def state_from_json(obj):
    if obj["Type"] == "Task":
        return TaskState.from_json(obj)
    elif obj["Type"] == "Succeed":
        return SucceedState.from_json(obj)
    elif obj["Type"] == "Fail":
        return FailState.from_json(obj)
    else:
        raise TypeError("Unknown type {}".format(obj["Type"]))