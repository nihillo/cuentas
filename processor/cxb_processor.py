import yaml
import pandas as pd
from processor.processor import Processor


COLUMNS = {
    'DATE': 'Fecha',
    'DATE_TRANSACTION': 'Fecha valor',
    'DESCRIPTION': 'Movimiento',
    'ADDITIONAL_DESCRIPTION': 'Más datos',
    'AMOUNT': 'Importe',
    'BALANCE': 'Saldo'
}

class CaixabankProcessor(Processor):
    def __init__(self):
        self.account = 'Caixabank'
        super().__init__()

    def process(self, data):       
        df = pd.read_excel(data, header=2)
        
        df[COLUMNS['DATE']] = pd.to_datetime(
            df[COLUMNS['DATE']],
            format='%d/%m/%Y'
        )
        
        df[self.CONFIG['OUTPUT_COLUMNS']['ACCOUNT']] = self.account
        df[self.CONFIG['OUTPUT_COLUMNS']['HASH'] ] = df.apply(lambda row: self.generate_hash(self.extract_row_hash_values(row, COLUMNS)), axis=1)

        df[COLUMNS['DESCRIPTION']] = df[COLUMNS['DESCRIPTION']] + ' - ' + df[COLUMNS['ADDITIONAL_DESCRIPTION']].fillna('')
        
        df = df\
            .drop(columns=[\
                COLUMNS['DATE_TRANSACTION'],\
                COLUMNS['ADDITIONAL_DESCRIPTION'],\
                COLUMNS['BALANCE']\
            ])\
            .rename(columns={\
                COLUMNS['DATE']: self.CONFIG['OUTPUT_COLUMNS']['DATE'],\
                COLUMNS['DESCRIPTION']: self.CONFIG['OUTPUT_COLUMNS']['DESCRIPTION'],\
                COLUMNS['AMOUNT']: self.CONFIG['OUTPUT_COLUMNS']['AMOUNT']
            })\
            .reset_index(drop=True)
        return df