from abc import ABC, abstractmethod
import hashlib, yaml

class Processor(ABC):
    def __init__(self):
        # Load config
        with open('config/config.yaml', 'r') as file:
            self.CONFIG = yaml.safe_load(file)

    
    @abstractmethod
    def process(self, data):
        pass
    
    def extract_row_hash_values(self, row, column_map):
        column_map.update({'ACCOUNT': self.CONFIG['OUTPUT_COLUMNS']['ACCOUNT']})
        
        return [
            str(row[column_map['DATE']]),
            str(row[column_map['DESCRIPTION']]),
            str(row[column_map['AMOUNT']]),
            str(row[column_map['BALANCE']]),
            str(row[column_map['ACCOUNT']])
        ]
    
    def generate_hash(self, values):
        hash_input = ''.join(values).encode('utf-8')
        return hashlib.md5(hash_input).hexdigest()