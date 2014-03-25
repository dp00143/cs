__author__ = 'Daniel'
# implementation uses from http://code.activestate.com/recipes/68429-ring-buffer/

class RingBuffer:
    def __init__(self,size_max):
        self.max = size_max
        self.data = []
    def append(self,x):
        self.data.append(x)
        if len(self.data) == self.max:
            self.cur = 0
            self.__class__ = RingBufferFull
    def get(self):
        return self.data
    def __len__(self):
        return len(self.data)

class RingBufferFull:
    def __init__(self,n):
        raise "Use RingBuffer instead!"
    def append(self,x):
        self.data[self.cur] = x
        self.cur = (self.cur+1) % self.max
    def get(self):
        return self.data[self.cur:]+self.data[:self.cur]
    def __len__(self):
        return self.max
