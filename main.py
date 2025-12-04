from calendar import month_abbr
from glob import glob
import yaml, os, sys
import pandas as pd
import processor.processor_factory as processor_factory
import categories.category_mgr as category_mgr

# Load config
with open('config/config.yaml', 'r') as file:
    CONFIG = yaml.safe_load(file)

def main():
    # Read first argument of script execution, if arg is unify, execute unify_transaction_files(), if it is reindex execute reindex_categories
    if len(sys.argv) > 1:
        if sys.argv[1] == 'unify':
            unify_transaction_files()
        elif sys.argv[1] == 'reindex':
            reindex_categories()
        elif sys.argv[1] == 'report':
            generate_report()
        else:
            print('Invalid operation argument')
    else:
        print('No operation argument provided')

    return
    
    

def unify_transaction_files():
    global CONFIG
    
    category_mgr_inst = category_mgr.get()

    if os.path.exists(os.path.join(CONFIG['OUTPUT_DIR'], CONFIG['OUTPUT_TRANSACTION_FILE'])):        
        df_out = pd.read_excel(os.path.join(CONFIG['OUTPUT_DIR'], CONFIG['OUTPUT_TRANSACTION_FILE']))
    else:       
        df_struct = {
            CONFIG['OUTPUT_COLUMNS']['DATE']: [],
            CONFIG['OUTPUT_COLUMNS']['DESCRIPTION']: [],
            CONFIG['OUTPUT_COLUMNS']['AMOUNT']: [],
            CONFIG['OUTPUT_COLUMNS']['CATEGORY']: []
        }

        df_out = pd.DataFrame(df_struct)      

    files = os.listdir(os.path.join(CONFIG['SOURCE_DIR'], CONFIG['TRANSACTIONS_DIR']))

    for file in files:
        if not (file.endswith('.xlsx') or file.endswith('.xls')):
            continue
        
        try:
            processor = processor_factory.get_processor(file)
            df_file = processor.process(os.path.join(CONFIG['SOURCE_DIR'], CONFIG['TRANSACTIONS_DIR'], file))
            
            existing_hashes = df_out[CONFIG['OUTPUT_COLUMNS']['HASH']]
            to_add_mask = ~df_file[CONFIG['OUTPUT_COLUMNS']['HASH']].isin(existing_hashes)
            rows_to_add = df_file[to_add_mask].copy()
            
            if rows_to_add.size > 0: 
                rows_to_add[CONFIG['OUTPUT_COLUMNS']['CATEGORY']] = rows_to_add.apply(lambda row: category_mgr_inst.categorize(row), axis=1)
            
            df_out = pd.concat([df_out, rows_to_add], ignore_index=True)  
        except Exception as e:
            print(e)
    
    df_out = df_out.sort_values(by=CONFIG['OUTPUT_COLUMNS']['DATE'], ascending=True)\
    
    df_out.to_excel(os.path.join(CONFIG['OUTPUT_DIR'], CONFIG['OUTPUT_TRANSACTION_FILE']), index=False)
    
    
def reindex_categories():
    category_mgr_inst = category_mgr.get()
    category_mgr_inst.reindex()
    
    
def generate_report():
    global CONFIG
    
    transaction_file = os.path.join(CONFIG['OUTPUT_DIR'], CONFIG['OUTPUT_TRANSACTION_FILE'])
    if not os.path.exists(transaction_file):
        print('No transaction file found')
        return    
     
    df_transactions = pd.read_excel(transaction_file)
    
    category_mgr_inst = category_mgr.get()
    computable_categories = category_mgr_inst.get_computable_categories()
    df_transactions = df_transactions[df_transactions[CONFIG['OUTPUT_COLUMNS']['CATEGORY']].isin(computable_categories)]

    
    df_transactions[CONFIG['OUTPUT_COLUMNS']['DATE']] = pd.to_datetime(df_transactions[CONFIG['OUTPUT_COLUMNS']['DATE']])

    df_transactions[CONFIG['OUTPUT_COLUMNS']['YEAR_MONTH']] = df_transactions[CONFIG['OUTPUT_COLUMNS']['DATE']].dt.to_period('M')
    
    df_transactions[CONFIG['OUTPUT_COLUMNS']['TRANSACTION_TYPE']] = df_transactions[CONFIG['OUTPUT_COLUMNS']['AMOUNT']]\
        .apply(lambda x: CONFIG['VALUES']['INCOME'] if x > 0 else CONFIG['VALUES']['EXPENSE'] )
        
    df_transactions[CONFIG['OUTPUT_COLUMNS']['ABS_AMOUNT']] = df_transactions[CONFIG['OUTPUT_COLUMNS']['AMOUNT']].abs()
    
    report_pivot = pd.pivot_table(
        df_transactions,
        values=CONFIG['OUTPUT_COLUMNS']['ABS_AMOUNT'],
        index=[CONFIG['OUTPUT_COLUMNS']['TRANSACTION_TYPE'], CONFIG['OUTPUT_COLUMNS']['CATEGORY']],
        columns=[CONFIG['OUTPUT_COLUMNS']['YEAR_MONTH']],
        aggfunc='sum',
        fill_value=0
    )
    
    all_types = df_transactions[CONFIG['OUTPUT_COLUMNS']['TRANSACTION_TYPE']].unique()
    all_categories = df_transactions[CONFIG['OUTPUT_COLUMNS']['CATEGORY']].unique()
    
    full_index = pd.MultiIndex.from_product(
        [all_types, all_categories],
        names=[CONFIG['OUTPUT_COLUMNS']['TRANSACTION_TYPE'], CONFIG['OUTPUT_COLUMNS']['CATEGORY']]
    )
    
    report_pivot = report_pivot.reindex(full_index, fill_value=0)
            
    report_pivot[CONFIG['OUTPUT_COLUMNS']['YEARLY_TOTAL']] = report_pivot.sum(axis=1)
    month_columns = report_pivot.columns.drop(CONFIG['OUTPUT_COLUMNS']['YEARLY_TOTAL'])
    report_pivot[CONFIG['OUTPUT_COLUMNS']['MONTHLY_AVG']] = report_pivot[month_columns].mean(axis=1).astype(float).round(2)
    
    income_by_category = report_pivot.loc[CONFIG['VALUES']['INCOME']]
    expenses_by_category = report_pivot.loc[CONFIG['VALUES']['EXPENSE']]
    
    df_balance_by_category = income_by_category - expenses_by_category

    df_balance_by_category.index.name = report_pivot.index.names[1]
    
    balance_index = pd.MultiIndex.from_product(
        [['Balance Categoria'], df_balance_by_category.index],
        names=report_pivot.index.names
    )
    
    df_balance_by_category.index = balance_index

    total_income = income_by_category.sum()
    total_expenses = expenses_by_category.sum()
    
    df_total_balance_by_month = total_income - total_expenses
    
    total_index = pd.MultiIndex.from_product(
        [['Balance Total'], ['TOTAL']],
        names=report_pivot.index.names
    )
    
    df_total_balance = pd.DataFrame(
        df_total_balance_by_month.values.reshape(1, -1),
        columns = report_pivot.columns,
        index = total_index
    )
    
    report_final = pd.concat([report_pivot, df_balance_by_category, df_total_balance])
    
    report_final.to_excel(os.path.join(CONFIG['OUTPUT_DIR'], CONFIG['OUTPUT_REPORT_FILE']), index=True)
    


if __name__ == '__main__':
    main()
    # generate_report()
    # unify_transaction_files()