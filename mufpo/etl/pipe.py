class Pipe:
    def __init__(self, value, failed=False):
        self.value = value
        self.failed = failed

    def get(self):
        return self.value
    
    def is_failed(self):
        return self.failed
    
    def __str__(self):
        return ' '.join([str(self.value), str(self.failed)])
    
    def bind(self, f, arg=False, kwarg=False):
        if self.failed:
            return self
        try:
            if arg:
                x = f(*self.get())
            elif kwarg:
                x = f(**self.get())
            else:
                x = f(self.get())
            return Pipe(x)
        except Exception as err:
            print(err, f.__name__)
            return Pipe(None, True)
    
    def __rshift__(self, f):
        return  self.bind(f, arg=True)
    
    def __gt__(self, f):
        return self.bind(f, kwarg=True)

    def __or__(self, f):
        return self.bind(f)