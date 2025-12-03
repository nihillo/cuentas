import yaml
import pandas as pd
from processor.processor import Processor

COLUMNS = {
    'DATE': 'FECHA OPERACIÓN',
    'DATE_TRANSACTION': 'FECHA VALOR',
    'DESCRIPTION': 'CONCEPTO',
    'AMOUNT': 'IMPORTE EUR',
    'BALANCE': 'SALDO'
}

class SantanderProcessor(Processor):
    def __init__(self):
        self.account = 'Santander'
        super().__init__()

    def process(self, data):
       
        df = pd.read_excel(data, header=7)
        
        df[COLUMNS['DATE']] = pd.to_datetime(
            df[COLUMNS['DATE']],
            format='%d/%m/%Y'
        )
        
        df[self.CONFIG['OUTPUT_COLUMNS']['ACCOUNT']] = self.account
        df[self.CONFIG['OUTPUT_COLUMNS']['HASH'] ] = df.apply(lambda row: self.generate_hash(self.extract_row_hash_values(row, COLUMNS)), axis=1)

        df = df\
            .drop(columns=[\
                COLUMNS['DATE_TRANSACTION'],\
                COLUMNS['BALANCE']\
            ])\
            .rename(columns={\
                COLUMNS['DATE']: self.CONFIG['OUTPUT_COLUMNS']['DATE'],\
                COLUMNS['DESCRIPTION']: self.CONFIG['OUTPUT_COLUMNS']['DESCRIPTION'],\
                COLUMNS['AMOUNT']: self.CONFIG['OUTPUT_COLUMNS']['AMOUNT']
            })\
            .reset_index(drop=True)
        return df