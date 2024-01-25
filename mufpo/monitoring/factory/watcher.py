from abc import ABC, abstractmethod


class Watcher(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def log(self):
        raise NotImplemented('This class need to be implement!')


