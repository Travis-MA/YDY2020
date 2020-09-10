import abc

#任务抽象类
class Task(metaclass = abc.ABCMeta):

    @abc.abstractmethod 
    def getType(self):
        pass

    @abc.abstractmethod 
    def run(self, para):
        pass


#定时任务抽象类
class ScheduleTask(Task, metaclass = abc.ABCMeta):

    @abc.abstractmethod
    def setPeriod(self, period):
        pass

