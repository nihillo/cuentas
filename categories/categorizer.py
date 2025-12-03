from abc import ABC, abstractmethod

class Categorizer(ABC):   
    @abstractmethod
    def categorize(self, row):
        pass
    
    def reindex(self):
        print(self.__str__() + ': this type of categorizer does not support reindexing')
        
    def __str__(self) -> str:
        return 'Abstract Categorizer'